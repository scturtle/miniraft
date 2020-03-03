#![allow(dead_code)]

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

#[derive(PartialEq)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

struct LeaderState {
    next_index: BTreeMap<i32, i32>,
    match_index: BTreeMap<i32, i32>,
}

struct ElectionState {
    term: i32,
    votes: i32,
}

struct Node {
    id: i32,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    peers: BTreeMap<i32, Sender<Msg>>,
    // persistent
    curr_term: i32,
    vote_for: Option<i32>,
    logs: Vec<LogEntry>,
    // all state
    commit_index: i32,
    last_applied: i32,
    state: NodeState,
    election_reset_event: Instant,
    // leader state
    leader_state: Option<LeaderState>,
    // tmp state
    election_state: Option<ElectionState>,
}

impl Node {
    pub fn new(id: i32) -> Node {
        let (sender, receiver) = channel(100);
        Node {
            id: id,
            sender: sender,
            receiver: receiver,
            peers: BTreeMap::new(),
            curr_term: 0,
            vote_for: None,
            logs: Vec::new(),
            commit_index: -1,
            last_applied: -1,
            state: NodeState::Follower,
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
        let mut interval = stream::interval(Duration::from_millis(10));
        loop {
            select! {
                _ = interval.next().fuse() => {
                    info!("{} tick", self.id);
                },
                msg = self.receiver.recv().fuse() => {
                    info!("{} recv", self.id);
                    match msg {
                        Some(Msg::RequestVoteArgs{..}) => {
                            // TODO
                        },
                        Some(Msg::RequestVoteReply(reply)) => {
                            self.process_vote_reply(reply);
                        },
                        Some(Msg::AppendEntriesArgs{..}) => {
                            // TODO
                        },
                        Some(Msg::AppendEntriesReply{..}) => {
                            // TODO
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn election_timeout(&self) -> Duration {
        Duration::from_millis(thread_rng().gen_range(150, 300))
    }

    async fn start_eletion(&mut self) {
        self.state = NodeState::Candidate;
        self.curr_term += 1;
        self.election_reset_event = Instant::now();
        self.vote_for = Some(self.id);
        self.election_state = Some(ElectionState {
            term: self.curr_term,
            votes: 1,
        });
        info!(
            "{} become candidate and start eletion at term {}",
            self.id, self.curr_term
        );
        // request vote to all other servers
        let msg = RequestVoteArgs {
            term: self.curr_term,
            candidate_id: self.id,
        };
        for (_peer_id, peer_sender) in self.peers.iter() {
            peer_sender.send(Msg::RequestVoteArgs(msg.clone())).await;
        }
    }

    fn process_vote_reply(&mut self, reply: RequestVoteReply) {
        if self.state != NodeState::Candidate {
            return;
        }
        let election_state = self.election_state.as_mut().unwrap();
        if reply.term > election_state.term {
            info!("{} update term {}", self.id, reply.term);
            // TODO become follower
            return;
        } else if reply.term == election_state.term && reply.vote_granted {
            election_state.votes += 1;
            if election_state.votes * 2 > self.peers.len() as i32 + 1 {
                // TODO start leader
            }
        }
    }

    fn become_follower(&mut self, term: i32) {
        info!("{} become follower with term {}", self.id, term);
        self.state = NodeState::Follower;
        self.curr_term = term;
        self.election_reset_event = Instant::now();
        self.election_state = None;
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
