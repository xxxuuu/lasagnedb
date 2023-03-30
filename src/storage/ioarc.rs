use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub struct IoArc<T>(Arc<T>);

impl<T> IoArc<T> {
    pub fn new(data: T) -> Self {
        Self(Arc::new(data))
    }

    pub fn from_arc(data: Arc<T>) -> Self {
        Self(data)
    }
}

impl<T> Read for IoArc<T>
where
    for<'a> &'a T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&mut &*self.0).read(buf)
    }
}

impl<T> Write for IoArc<T>
where
    for<'a> &'a T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&mut &*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&mut &*self.0).flush()
    }
}

impl<T> Seek for IoArc<T>
where
    for<'a> &'a T: Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&mut &*self.0).seek(pos)
    }
}

impl<T> Deref for IoArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}
