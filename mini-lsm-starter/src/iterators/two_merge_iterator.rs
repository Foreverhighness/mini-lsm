#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::needless_pass_by_value)] // TODO(fh): remove clippy allow

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    using_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            using_a: false,
        };
        iter.update_using()?;
        Ok(iter)
    }

    fn update_using(&mut self) -> Result<()> {
        if self.using_a {
            if !self.a.is_valid() {
                self.using_a = !self.using_a;
            } else if self.b.is_valid() {
                debug_assert!(self.using_a);
                let ord = self.a.key().cmp(&self.b.key());
                self.using_a = match ord {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => {
                        self.b.next()?;
                        true
                    }
                    std::cmp::Ordering::Greater => false,
                };
            }
        } else if !self.b.is_valid() {
            self.using_a = !self.using_a;
        } else if self.a.is_valid() {
            debug_assert!(!self.using_a);
            let ord = self.a.key().cmp(&self.b.key());
            self.using_a = match ord {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => {
                    self.b.next()?;
                    true
                }
                std::cmp::Ordering::Greater => false,
            };
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.using_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.using_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.using_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());

        if self.using_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.update_using()?;

        Ok(())
    }
}
