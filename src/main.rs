#![allow(dead_code)]
#![recursion_limit = "256"]

use async_std::sync::{channel, Receiver, Sender};
use async_std::{stream, stream::StreamExt};
use futures::{future::FutureExt, join, select};
use log::info;
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

struct LogEntry {
    cmd: String,
    term: i32,
}

#[derive(Clone)]
struct RequestVoteArgs {
    term: i32,
    candidate_id: i32,
    // last_log_index: i32,
    // last_log_term: i32,
}

struct RequestVoteReply {
    term: i32,
    vote_granted: bool,
}

#[derive(Clone)]
struct AppendEntriesArgs {
    term: i32,
    leader_id: i32,
    // prev_log_index: i32,
    // prev_log_term: i32,
    // entries: Vec<LogEntry>,
    // leader_commit: i32,
}

struct AppendEntriesReply {
    term: i32,
    success: bool,
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
    // next_index: BTreeMap<i32, i32>,
    // match_index: BTreeMap<i32, i32>,
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
    // logs: Vec<LogEntry>,
    // all state
    // commit_index: i32,
    // last_applied: i32,
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
            // logs: Vec::new(),
            // commit_index: -1,
            // last_applied: -1,
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
                    } else {
                        // if role is follower and not received heartbeat for a lone time or
                        // if role is candidate and not received major votes in this term
                        if self.election_reset_event.elapsed() > election_timeout {
                            self.start_eletion().await;
                            election_timeout = self.get_election_timeout(); // re-random
                        }
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
        if args.term == self.curr_term
            && (self.vote_for.is_none() || self.vote_for == Some(args.candidate_id))
        {
            info!("{} vote for {}", self.id, args.candidate_id);
            reply.vote_granted = true;
            self.vote_for = Some(args.candidate_id);
            self.election_reset_event = Instant::now(); // postpond
        }
        let peer_sender = self.peers.get(&args.candidate_id).unwrap();
        peer_sender.send(Msg::RequestVoteReply(reply)).await;
    }

    async fn process_append_entries_request(&mut self, args: AppendEntriesArgs) {
        if args.term > self.curr_term {
            self.become_follower(args.term);
        }
        let mut reply = AppendEntriesReply {
            term: self.curr_term,
            success: false,
        };
        if args.term == self.curr_term {
            if self.role == NodeRole::Candidate {
                // someone else win this term
                self.become_follower(args.term);
            }
            self.election_reset_event = Instant::now();
            reply.success = true;
        }
        let peer_sender = self.peers.get(&args.leader_id).unwrap();
        peer_sender.send(Msg::AppendEntriesReply(reply)).await;
    }

    fn process_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term > self.curr_term {
            self.become_follower(reply.term);
        }
    }

    fn get_election_timeout(&self) -> Duration {
        Duration::from_millis(thread_rng().gen_range(150, 300))
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
        // request vote to all peers
        let args = RequestVoteArgs {
            term: self.curr_term,
            candidate_id: self.id,
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
        self.leader_state = Some(LeaderState {
            // next_index: BTreeMap::new(),
            // match_index: BTreeMap::new(),
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
        let args = AppendEntriesArgs {
            term: self.curr_term,
            leader_id: self.id,
        };
        for peer_sender in self.peers.values() {
            peer_sender.send(Msg::AppendEntriesArgs(args.clone())).await;
        }
    }
}

async fn test() {
    let mut node1 = Node::new(1);
    let mut node2 = Node::new(2);
    let mut node3 = Node::new(3);
    node1.add_peer(&node2);
    node1.add_peer(&node3);
    node2.add_peer(&node1);
    node2.add_peer(&node3);
    node3.add_peer(&node1);
    node3.add_peer(&node2);
    join!(node1.run(), node2.run(), node3.run());
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();
    async_std::task::block_on(test());
}
