#![allow(dead_code)]
#![recursion_limit = "256"]

use async_std::sync::{channel, Receiver, Sender};
use async_std::{stream, stream::StreamExt};
use futures::{future::FutureExt, select};
use log::info;
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct LogEntry {
    cmd: String,
    term: i32,
}

#[derive(Clone)]
struct RequestVoteArgs {
    term: i32,
    candidate_id: i32,
    last_log_index: i32,
    last_log_term: i32,
}

struct RequestVoteReply {
    term: i32,
    vote_granted: bool,
}

struct AppendEntriesArgs {
    term: i32,
    leader_id: i32,
    prev_log_index: i32,
    prev_log_term: i32,
    entries: Vec<LogEntry>,
    leader_commit: i32,
}

struct AppendEntriesReply {
    term: i32,
    success: bool,
    peer_id: i32,    // extra
    next_index: i32, // extra
}

enum Msg {
    RequestVoteArgs(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesArgs(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
}

#[derive(PartialEq, Debug)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

struct LeaderState {
    next_index: BTreeMap<i32, i32>,
    match_index: BTreeMap<i32, i32>,
    last_heartbeat_event: Instant,
}

struct ElectionState {
    votes: i32, // in curr term
}

struct Node {
    id: i32,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    peers: BTreeMap<i32, Sender<Msg>>, // peer id => sender
    // persistent
    curr_term: i32,
    vote_for: Option<i32>, // in curr term
    logs: Vec<LogEntry>,
    // all state
    commit_index: i32,
    last_applied: i32,
    role: NodeRole,
    election_reset_event: Instant,
    // tmp state
    leader_state: Option<LeaderState>, // valid if role is leader
    election_state: Option<ElectionState>, // valid if role is candidate
}

impl Node {
    pub fn new(id: i32) -> Node {
        let (sender, receiver) = channel(100);
        Node {
            id,
            sender,
            receiver,
            peers: BTreeMap::new(),
            curr_term: 0,
            vote_for: None,
            logs: Vec::new(),
            commit_index: -1,
            last_applied: -1,
            role: NodeRole::Follower,
            election_reset_event: Instant::now(),
            leader_state: None,
            election_state: None,
        }
    }

    pub fn add_peer(&mut self, node: &Node) {
        assert_ne!(self.id, node.id);
        self.peers.insert(node.id, node.sender.clone());
    }

    pub async fn run(&mut self) {
        let mut check_interval = stream::interval(Duration::from_millis(10));
        let heartbeat_interval = Duration::from_millis(50);
        let mut election_timeout = self.get_election_timeout();
        loop {
            select! {
                _ = check_interval.next().fuse() => {
                    if self.role == NodeRole::Leader {
                        // send periodic heartbeats
                        let leader_state = self.leader_state.as_mut().unwrap();
                        if leader_state.last_heartbeat_event.elapsed() > heartbeat_interval {
                            leader_state.last_heartbeat_event = Instant::now();
                            self.send_heartbeat().await;
                        }
                        // random log entry nearly every 1 sec
                        if thread_rng().gen::<u8>() <= 2 {
                            self.logs.push(LogEntry{
                                cmd: thread_rng().gen::<u8>().to_string(),
                                term: self.curr_term,
                            });
                            info!("{} new log {}", self.id, self.logs.len() - 1);
                        }
                    } else {
                        // if role is follower and not received heartbeat for a lone time or
                        // if role is candidate and not received major votes in this term
                        if self.election_reset_event.elapsed() > election_timeout {
                            self.start_eletion().await;
                            election_timeout = self.get_election_timeout(); // re-random
                        }
                    }
                    // apply the committed log entires
                    if self.commit_index > self.last_applied {
                        let indices = (self.last_applied+1..self.commit_index+1).collect::<Vec<_>>();
                        info!("{} applied logs {:?}", self.id, indices);
                        self.last_applied = self.commit_index;
                    }
                },
                msg = self.receiver.recv().fuse() => {
                    if let Some(msg) = msg {
                        self.dispatch_message(msg).await;
                    } else {
                        break;  // channel is closed?
                    }
                }
            }
        }
    }

    async fn dispatch_message(&mut self, msg: Msg) {
        match msg {
            Msg::RequestVoteArgs(args) => {
                self.process_vote_request(args).await;
            }
            Msg::RequestVoteReply(reply) => {
                self.process_vote_reply(reply).await;
            }
            Msg::AppendEntriesArgs(args) => {
                self.process_append_entries_request(args).await;
            }
            Msg::AppendEntriesReply(reply) => {
                self.process_append_entries_reply(reply);
            }
        }
    }

    async fn process_vote_request(&mut self, args: RequestVoteArgs) {
        if args.term > self.curr_term {
            self.become_follower(args.term);
        }
        let mut reply = RequestVoteReply {
            term: self.curr_term,
            vote_granted: false,
        };
        let (last_log_index, last_log_term) = self.last_log_index_and_term();
        if args.term == self.curr_term
            && (self.vote_for.is_none() || self.vote_for == Some(args.candidate_id))
            && (args.last_log_term > last_log_term
                || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index))
        {
            info!("{} vote for {}", self.id, args.candidate_id);
            reply.vote_granted = true;
            self.vote_for = Some(args.candidate_id);
            self.election_reset_event = Instant::now(); // postpond
        }
        self.peers[&args.candidate_id]
            .send(Msg::RequestVoteReply(reply))
            .await;
    }

    async fn process_append_entries_request(&mut self, args: AppendEntriesArgs) {
        if args.term > self.curr_term {
            self.become_follower(args.term);
        }
        let mut reply = AppendEntriesReply {
            peer_id: self.id,
            term: self.curr_term,
            success: false,
            next_index: 0,
        };
        if args.term == self.curr_term {
            if self.role == NodeRole::Candidate {
                // someone else win this term
                self.become_follower(args.term);
            }
            self.election_reset_event = Instant::now();

            if args.prev_log_index == -1
                || (args.prev_log_index < self.logs.len() as i32
                    && args.prev_log_term == self.logs[args.prev_log_index as usize].term)
            {
                // prev log matched
                reply.success = true;
                reply.next_index = args.prev_log_index + args.entries.len() as i32 + 1;
                // find the mismatch point
                let mut log_insert_index = (args.prev_log_index + 1) as usize;
                let mut new_entries_index = 0;
                loop {
                    if log_insert_index >= self.logs.len()
                        || new_entries_index >= args.entries.len()
                    {
                        break;
                    }
                    if self.logs[log_insert_index].term != args.entries[new_entries_index].term {
                        break;
                    }
                    log_insert_index += 1;
                    new_entries_index += 1;
                }
                // drop self.logs[log_insert_index..]
                let _ = self.logs.drain(log_insert_index..);
                // add args.entries[new_entries_index..]
                let new_entries = &args.entries[new_entries_index..];
                self.logs.extend_from_slice(new_entries);
                if !new_entries.is_empty() {
                    let new_indices = (log_insert_index..self.logs.len()).collect::<Vec<_>>();
                    info!("{} accept logs {:?}", self.id, new_indices);
                }
                // set commit index
                if args.leader_commit > self.commit_index {
                    let old_commit_index = self.commit_index;
                    self.commit_index = args.leader_commit.min(self.logs.len() as i32 - 1);
                    info!(
                        "{} commit logs {:?}",
                        self.id,
                        (old_commit_index + 1..self.commit_index + 1).collect::<Vec<_>>()
                    );
                }
            }
        }
        self.peers[&args.leader_id]
            .send(Msg::AppendEntriesReply(reply))
            .await;
    }

    fn process_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term > self.curr_term {
            self.become_follower(reply.term);
        }
        if self.role == NodeRole::Leader {
            let leader_state = self.leader_state.as_mut().unwrap();
            let peer_next_index = leader_state.next_index.get_mut(&reply.peer_id).unwrap();
            let peer_match_index = leader_state.match_index.get_mut(&reply.peer_id).unwrap();
            if !reply.success {
                // if pre log index not matched, decrease index by 1
                *peer_next_index -= 1;
            } else {
                *peer_next_index = reply.next_index;
                *peer_match_index = reply.next_index - 1;
                for i in self.commit_index + 1..*peer_match_index + 1 {
                    if self.logs[i as usize].term == self.curr_term {
                        let match_cnt = 1 + self
                            .peers
                            .keys()
                            .filter(|peer_id| leader_state.match_index[peer_id] >= i)
                            .count();
                        // if replicated by a majority then commit it
                        if match_cnt * 2 > self.peers.len() + 1 {
                            self.commit_index = i;
                            info!("{} commit log {} ({} replicates)", self.id, i, match_cnt);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn get_election_timeout(&self) -> Duration {
        Duration::from_millis(thread_rng().gen_range(150, 300))
    }

    fn last_log_index_and_term(&self) -> (i32, i32) {
        let last_log_index = self.logs.len() as i32 - 1;
        let last_log_term = if last_log_index >= 0 {
            self.logs[last_log_index as usize].term
        } else {
            -1
        };
        (last_log_index, last_log_term)
    }

    async fn start_eletion(&mut self) {
        // become candidate
        self.role = NodeRole::Candidate;
        self.curr_term += 1;
        self.election_reset_event = Instant::now();
        self.vote_for = Some(self.id);
        // self.leader_state = None;  // cannot be leader
        self.election_state = Some(ElectionState { votes: 1 });
        info!(
            "{} become Candidate and start election at term {}",
            self.id, self.curr_term
        );
        let (last_log_index, last_log_term) = self.last_log_index_and_term();
        // request vote to all peers
        let args = RequestVoteArgs {
            term: self.curr_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        for peer_sender in self.peers.values() {
            peer_sender.send(Msg::RequestVoteArgs(args.clone())).await;
        }
    }

    async fn process_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term > self.curr_term {
            self.become_follower(reply.term);
        }
        if self.role != NodeRole::Candidate {
            return;
        }
        let election_state = self.election_state.as_mut().unwrap();
        if reply.term == self.curr_term && reply.vote_granted {
            election_state.votes += 1;
            self.election_reset_event = Instant::now(); // NOTE: wait for more votes
            if election_state.votes * 2 > self.peers.len() as i32 + 1 {
                info!(
                    "{} become Leader with votes {} at term {}",
                    self.id, election_state.votes, self.curr_term
                );
                self.become_leader().await;
            }
        }
    }

    async fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        // self.election_state = None;
        let next_log_index = self.logs.len() as i32;
        let next_index = self.peers.keys().map(|&id| (id, next_log_index)).collect();
        let match_index = self.peers.keys().map(|&id| (id, -1)).collect();
        self.leader_state = Some(LeaderState {
            next_index,
            match_index,
            last_heartbeat_event: Instant::now(),
        });
        self.send_heartbeat().await;
    }

    fn become_follower(&mut self, term: i32) {
        info!(
            "{} become Follower with term {} (from {:?} in term {})",
            self.id, term, self.role, self.curr_term
        );
        self.role = NodeRole::Follower;
        self.vote_for = None;
        self.curr_term = term;
        self.election_reset_event = Instant::now();
        // self.election_state = None;
        // self.leader_state = None;
    }

    async fn send_heartbeat(&self) {
        let leader_state = self.leader_state.as_ref().unwrap();
        for (peer_id, peer_sender) in &self.peers {
            let i = leader_state.next_index[peer_id];
            let prev_log_index = i - 1;
            let prev_log_term = if prev_log_index >= 0 {
                self.logs[prev_log_index as usize].term
            } else {
                -1
            };
            let args = AppendEntriesArgs {
                term: self.curr_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: self.logs[i as usize..].to_vec(),
                leader_commit: self.commit_index,
            };
            peer_sender.send(Msg::AppendEntriesArgs(args)).await;
        }
    }
}

async fn test() {
    use core::cell::RefCell;
    use futures::stream::futures_unordered::FuturesUnordered;
    let nodes: Vec<RefCell<Node>> = (0..5).map(|i| RefCell::new(Node::new(i))).collect();
    for i in 0..5 {
        for j in 0..5 {
            if i != j {
                nodes[i].borrow_mut().add_peer(&nodes[j].borrow());
            }
        }
    }
    let mut nodes: Vec<Node> = nodes.into_iter().map(|node| node.into_inner()).collect();
    let futs: FuturesUnordered<_> = nodes.iter_mut().map(|node| node.run()).collect();
    futs.collect::<Vec<_>>().await;
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();
    async_std::task::block_on(test());
}
