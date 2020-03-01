use log::info;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::mpsc::{channel, Receiver, Sender},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
};

pub type ServerId = u32;
type MsgId = u32;
type FutKey = (ServerId, MsgId);
#[derive(PartialEq)]
enum MsgType { REQ, REP }
struct Message(FutKey, MsgType, String); // true for request, false for reply

struct ServerState {
    peer_senders: HashMap<ServerId, Sender<Message>>,
    fut_states: HashMap<FutKey, Arc<Mutex<ReplyState>>>,
}

pub struct RPCServer {
    pub id: ServerId,
    sender: Sender<Message>,
    state: Arc<Mutex<ServerState>>,
}

impl RPCServer {
    pub fn new(id: ServerId) -> Self {
        let (sender, receiver) = channel();
        let state = Arc::new(Mutex::new(ServerState {
            peer_senders: HashMap::new(),
            fut_states: HashMap::new(),
        }));

        let thread_state = state.clone();
        thread::spawn(move || RPCServer::run(id, receiver, thread_state));

        RPCServer { id, sender, state }
    }

    fn run(id: ServerId, receiver: Receiver<Message>, state: Arc<Mutex<ServerState>>) {
        loop {
            if let Ok(Message(fut_key, typ, msg)) = receiver.recv() {
                let mut state = state.lock().unwrap();
                match typ {
                    MsgType::REQ => {
                        let peer_sender = state
                            .peer_senders
                            .get(&fut_key.0)
                            .expect("peer sender not found")
                            .clone();
                        peer_sender
                            .send(Message((id, fut_key.1), MsgType::REP, msg))
                            .expect("send error");
                    },
                    MsgType::REP => {
                        let fut_state = state.fut_states.remove(&fut_key).expect("fut not found");
                        fut_state.lock().unwrap().wake(msg);
                    }
                }
            } else {
                break;
            }
        }
    }

    pub fn add_peer(&self, peer: &RPCServer) {
        let mut state = self.state.lock().unwrap();
        state.peer_senders.insert(peer.id, peer.sender.clone());
    }

    pub async fn send_request(&self, peer_id: ServerId, msg_id: MsgId, msg: String) -> String {
        let mut state = self.state.lock().unwrap();
        let peer_sender = state
            .peer_senders
            .get(&peer_id)
            .expect("peer sender not found");
        peer_sender
            .send(Message((self.id, msg_id), MsgType::REQ, msg))
            .expect("send error");
        let fut_key = (peer_id, msg_id);
        assert!(!state.fut_states.contains_key(&fut_key), "msg sent already");
        let fut = ReplyFuture::new();
        state.fut_states.insert(fut_key, fut.get_state());
        drop(state); // unlock
        fut.await
    }
}

struct ReplyState {
    reply: Option<String>,
    waker: Option<Waker>,
}

impl ReplyState {
    fn wake(&mut self, msg: String) {
        self.reply = Some(msg);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

struct ReplyFuture {
    state: Arc<Mutex<ReplyState>>,
}

impl ReplyFuture {
    fn new() -> Self {
        let state = Arc::new(Mutex::new(ReplyState {
            reply: None,
            waker: None,
        }));
        ReplyFuture { state }
    }
    fn get_state(&self) -> Arc<Mutex<ReplyState>> {
        self.state.clone()
    }
}

impl Future for ReplyFuture {
    type Output = String;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        if let Some(reply) = state.reply.take() {
            Poll::Ready(reply)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

async fn test() {
    let sv1 = RPCServer::new(1);
    let sv2 = RPCServer::new(2);
    sv1.add_peer(&sv2);
    sv2.add_peer(&sv1);
    let req = "hello".to_string();
    info!("req: {}", &req);
    let rep = sv1.send_request(sv2.id, 0, req).await;
    info!("rep: {}", rep);
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();
    async_std::task::block_on(test());
}
