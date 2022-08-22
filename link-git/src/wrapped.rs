mod incoming;

#[async_trait]
pub trait Process {
    type Stdin;
    type Stdout;
    type Stderr;

    fn stdin(&mut self) -> &mut Option<Self::Stdin>;
    fn stdout(&mut self) -> &mut Option<Self::Stdout>;
    fn stderr(&mut self) -> &mut Option<Self::Stderr>;
    fn id(&self) -> Option<u32>;
    async fn kill(&mut self) -> Result<(), std::io::Error>;
    async fn wait(&mut self) -> Result<std::process::ExitStatus, std::io::Error>;
}

pub struct Wrapped<P: Process> {
    pub stdin: Option<WrappedStdIn<P::Stdin>>,
    pub stdout: Option<WrappedStdOut<P::Stdout>>,
    pub stderr: Option<P::Stderr>,
    inner: P,
}
impl<P: Process> Wrapped<P> 
{

    pub fn id(&self) -> Option<u32> {
        self.inner.id()
    }

    pub async fn kill(&mut self) -> std::io::Result<()> {
        self.inner.kill().await
    }

    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.inner.wait().await
    }
}

pub struct WrappedStdOut<O>{
    inner: O,
}

pub struct WrappedStdIn<I> {
    state: incoming::IncomingState,
    inner: I,
}

pub fn wrap<P: Process>(mut process: P) -> Wrapped<P> {
    let stdout = process.stdout().take().map(|s| WrappedStdOut{inner: s});
    let stdin = process.stdin().take().map(|s| WrappedStdIn{
        state: incoming::IncomingState::new(),
        inner: s
    });
    let stderr = process.stderr().take();
    Wrapped{
        stdout,
        stdin,
        stderr,
        inner: process,
    }
}

#[cfg(feature = "tokio")]
mod tokio_imp {
    use tokio::io::{AsyncRead, AsyncWriteExt};
    use super::*;

    #[async_trait]
    impl Process for tokio::process::Child {
        type Stdin = tokio::process::ChildStdin;
        type Stdout = tokio::process::ChildStdout;
        type Stderr = tokio::process::ChildStderr;

        fn stdin(&mut self) -> &mut Option<Self::Stdin> {
            &mut self.stdin
        }

        fn stdout(&mut self) -> &mut Option<Self::Stdout> {
            &mut self.stdout
        }

        fn stderr(&mut self) -> &mut Option<Self::Stderr> {
            &mut self.stderr
        }

        fn id(&self) -> Option<u32> {
            tokio::process::Child::id(self)
        }

        async fn kill(&mut self) -> Result<(), std::io::Error> {
            tokio::process::Child::kill(self).await
        }

        async fn wait(&mut self) -> Result<std::process::ExitStatus, std::io::Error> {
            tokio::process::Child::wait(self).await
        }

    }

    impl WrappedStdIn<tokio::process::ChildStdin> {
        pub async fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
            let new_bytes = self.state.with_data(data);
            self.inner.write_all(&new_bytes).await 
        }

        pub async fn shutdown(&mut self) -> std::io::Result<()> {
            self.inner.shutdown().await
        }
    }

    impl AsyncRead for WrappedStdOut<tokio::process::ChildStdout> {
        fn poll_read(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_read(cx, buf)
        }
    }
}
