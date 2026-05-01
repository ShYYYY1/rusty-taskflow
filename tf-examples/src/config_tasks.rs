use taskflow::sync_task;

pub struct FibSource1;

impl FibSource1 {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl FibSource1 {
    fn run(self) -> u8 {
        10
    }
}

pub struct FibSource2;

impl FibSource2 {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl FibSource2 {
    fn run(self) -> u8 {
        10
    }
}

pub struct Merger;

impl Merger {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl Merger {
    fn run(self, v1: &u8, v2: &u8) -> u8 {
        v1 + v2
    }
}

pub struct Multiply {
    factor: u8,
}

impl Multiply {
    pub fn new() -> Self {
        Self { factor: 2 }
    }
}

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, v: &u8) -> u8 {
        self.factor * v
    }
}
