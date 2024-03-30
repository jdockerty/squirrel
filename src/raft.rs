use raft::prelude::Message;

pub type ProposeCallback = Box<dyn Fn() + Send>;

pub enum Msg {
    Propose { id: u8, callback: ProposeCallback },
    Raft(Message),
}
