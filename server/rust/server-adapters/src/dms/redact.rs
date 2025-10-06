use std::{fmt, marker::PhantomData};

pub struct Redacted<'a, T>(PhantomData<&'a T>);

impl<'a, T> Redacted<'a, T> {
    pub fn new(_: &'a T) -> Self {
        Self(PhantomData)
    }
}

impl<'a, T> fmt::Debug for Redacted<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

pub struct RedactedOption<'a>(Option<&'a str>);

impl<'a> RedactedOption<'a> {
    pub fn new(value: Option<&'a str>) -> Self {
        Self(value)
    }
}

impl<'a> fmt::Debug for RedactedOption<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(_) => f.write_str("[REDACTED]"),
            None => f.write_str("None"),
        }
    }
}

impl<'a> fmt::Display for RedactedOption<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(_) => f.write_str("[REDACTED]"),
            None => f.write_str("None"),
        }
    }
}
