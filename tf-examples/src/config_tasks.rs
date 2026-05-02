use taskflow::sync_task;

pub struct FibSource1;

impl FibSource1 {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl FibSource1 {
    fn run(self) ->u64 {
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
    fn run(self) ->u64 {
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
    fn run(self, v1: &u64, v2: &u64) ->u64 {
        println!("Merger output: {}", v1 + v2);
        v1 + v2
    }
}

pub struct Fib;
#[sync_task(path = "::taskflow")]
impl Fib {
    pub fn new() -> Self {
        Self
    }

    fn fib(v: &u64) ->u64 {
        if *v <= 1u64 {
            return *v;
        }
        Self::fib(&(*v - 1)) + Self::fib(&(*v - 2))
    }

    fn run(self, v: &u64) ->u64 {
        let res = Self::fib(v);
        println!("Fib result: {res}");
        res
    }
}

pub struct Multiply {
    factor:u64,
}

impl Multiply {
    pub fn new() -> Self {
        Self { factor: 2 }
    }
}

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, v: &u64) ->u64 {
        self.factor * v
    }
}
