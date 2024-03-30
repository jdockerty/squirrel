pub type ProposeCallback = Box<dyn Fn() + Send>;

pub enum Msg {
    Propose { id: u8, callback: ProposeCallback },
    Set { id: u8, key: String, value: String },
}
