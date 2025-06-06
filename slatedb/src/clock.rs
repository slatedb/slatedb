use std::time::SystemTime;

use crate::utils::system_time_to_millis;

pub trait SystemClock {
    fn now(&self) -> SystemTime;
}

pub struct DefaultSystemClock;

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

pub trait LogicalClock {
    fn now(&self) -> i64;
}

pub struct DefaultLogicalClock;

impl LogicalClock for DefaultLogicalClock {
    fn now(&self) -> i64 {
        system_time_to_millis(SystemTime::now())
    }
}