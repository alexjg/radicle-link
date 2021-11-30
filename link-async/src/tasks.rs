// Copyright © 2021 The Radicle Foundation <hello@radicle.foundation>
// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use core::time::Duration;
use futures::{
    stream::{FuturesUnordered, StreamExt},
    Future,
    FutureExt,
    Stream,
};
use std::{pin::Pin, task::Poll};

/// Run tasks from a stream of tasks but terminate if the stream is idle for
/// `idle_timeout`. The idle timeout starts when there are no tasks running and
/// no new tasks to pull from the stream.
///
/// If the stream returns None this will drive all current tasks to completion
/// and then exit.
pub fn run_until_idle<'a, T: 'a>(
    tasks: Pin<Box<dyn Stream<Item = crate::Task<T>> + Send + 'a>>,
    idle_timeout: Duration,
) -> impl futures::Future<Output = ()> + 'a {
    MortalServer {
        tasks,
        idle_timeout,
        state: MortalServerState::Idle(crate::sleep(idle_timeout).boxed()),
    }
}

/// Run a stream of tasks until the stream returns None, at which point all
/// remaining tasks will be driven to completion
pub fn run_forever<'a, T: 'a>(
    tasks: Pin<Box<dyn Stream<Item = crate::Task<T>> + Send + 'a>>,
) -> impl futures::Future<Output = ()> + 'a {
    ImmortalServer {
        new_tasks: tasks,
        ongoing_tasks: FuturesUnordered::new(),
        finishing: false,
    }
}

struct ImmortalServer<'a, T> {
    new_tasks: Pin<Box<dyn Stream<Item = crate::Task<T>> + Send + 'a>>,
    ongoing_tasks: FuturesUnordered<crate::Task<T>>,
    finishing: bool,
}

impl<'a, T> futures::Future for ImmortalServer<'a, T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if !self.finishing {
            while let Poll::Ready(maybe_task) = self.new_tasks.poll_next_unpin(cx) {
                match maybe_task {
                    Some(task) => {
                        self.ongoing_tasks.push(task);
                    },
                    None => {
                        self.finishing = true;
                        break;
                    },
                }
            }
        }
        while let Poll::Ready(Some(next_result)) = self.ongoing_tasks.poll_next_unpin(cx) {
            if let Err(e) = next_result {
                tracing::error!(err=?e, "error in connection");
            }
        }
        if self.finishing && self.ongoing_tasks.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct MortalServer<'a, T> {
    tasks: Pin<Box<dyn Stream<Item = crate::Task<T>> + Send + 'a>>,
    idle_timeout: Duration,
    state: MortalServerState<T>,
}

enum MortalServerState<T> {
    Servicing(FuturesUnordered<crate::Task<T>>),
    Finishing(FuturesUnordered<crate::Task<T>>),
    Idle(Pin<Box<dyn Future<Output = ()> + Send>>),
    Dead,
}

impl<'a, T> futures::Future for MortalServer<'a, T> {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if matches!(
            self.state,
            MortalServerState::Idle(..) | MortalServerState::Servicing(..)
        ) {
            let mut new_tasks = Vec::new();
            let mut finish = false;
            while let Poll::Ready(maybe_task) = self.tasks.poll_next_unpin(cx) {
                match maybe_task {
                    Some(task) => {
                        new_tasks.push(task);
                    },
                    None => {
                        finish = true;
                        break;
                    },
                }
            }
            if let MortalServerState::Servicing(ongoing_tasks) = &mut self.state {
                ongoing_tasks.extend(new_tasks);
                if finish {
                    self.state = MortalServerState::Finishing(std::mem::take(ongoing_tasks));
                }
            } else if !new_tasks.is_empty() {
                if finish {
                    self.state = MortalServerState::Finishing(new_tasks.into_iter().collect());
                } else {
                    self.state = MortalServerState::Servicing(new_tasks.into_iter().collect());
                }
            }
        }
        match &mut self.state {
            MortalServerState::Servicing(ongoing_tasks) => {
                while let Poll::Ready(Some(next_result)) = ongoing_tasks.poll_next_unpin(cx) {
                    if let Err(e) = next_result {
                        tracing::error!(err=?e, "error in connection");
                    }
                }
                if ongoing_tasks.is_empty() {
                    let mut sleep = crate::sleep(self.idle_timeout).boxed();
                    #[allow(unused_must_use)]
                    {
                        // Schedule waker for the sleep
                        sleep.poll_unpin(cx);
                    }
                    self.state = MortalServerState::Idle(sleep);
                }
                Poll::Pending
            },
            MortalServerState::Finishing(tasks) => {
                while let Poll::Ready(next_result) = tasks.poll_next_unpin(cx) {
                    if let Some(Err(e)) = next_result {
                        tracing::error!(err=?e, "error in connection");
                    }
                }
                if tasks.is_empty() {
                    self.state = MortalServerState::Dead;
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            },
            MortalServerState::Idle(sleep) => match sleep.poll_unpin(cx) {
                Poll::Ready(_) => {
                    self.state = MortalServerState::Dead;
                    Poll::Ready(())
                },
                _ => Poll::Pending,
            },
            MortalServerState::Dead => Poll::Ready(()),
        }
    }
}
