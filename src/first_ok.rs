use futures::Future;
use hyper::Uri;

/// Executes the given closure with each cluster member and short-circuit returns the first
/// successful result. If all members are exhausted without success, a vector of all errors is
/// returned.
pub async fn first_ok<F, G, T, E>(endpoints: Vec<Uri>, callback: F) -> Result<T, Vec<E>>
where
    F: FnMut(&Uri) -> G,
    G: Future<Output = Result<T, E>>,
{
    first_future_ok(endpoints.iter().map(callback)).await
}

/// Await all TryFutures in sequence, returning the result (and short-circuiting) if one
/// completes successfully or a vector of all errors if none does.
async fn first_future_ok<I, T, E>(futures: I) -> Result<T, Vec<E>>
where
    I: IntoIterator,
    I::Item: Future<Output = Result<T, E>>,
{
    let mut errors: Vec<E> = Vec::new();
    for future in futures {
        match future.await {
            Ok(item) => return Ok(item),
            Err(err) => {
                errors.push(err);
            }
        }
    }
    Err(errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::future::{ready, Ready};
    use std::sync::{atomic, Arc};

    #[test]
    fn test_first_ok_ok() {
        let futures = vec![ready(Err(0)), ready(Ok(1)), ready(Ok(2))];
        let actual = block_on(first_future_ok(futures));
        assert_eq!(actual, Ok(1));
    }

    #[test]
    fn test_first_ok_err() {
        let futures: Vec<Ready<Result<usize, usize>>> = vec![ready(Err(1)), ready(Err(2))];
        let actual = block_on(first_future_ok(futures));
        assert_eq!(actual, Err(vec![1, 2]));
    }

    async fn bump_count(count: Arc<atomic::AtomicUsize>) -> Result<usize, usize> {
        let value = count.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        if value == 1 {
            Ok(value)
        } else {
            Err(value)
        }
    }

    #[test]
    fn test_first_ok_short_circuit() {
        let count = Arc::new(atomic::AtomicUsize::new(0));
        let futures = vec![
            bump_count(count.clone()),
            bump_count(count.clone()),
            bump_count(count.clone()),
            bump_count(count.clone()),
        ];
        let actual = block_on(first_future_ok(futures));
        assert_eq!(actual, Ok(1));
        assert_eq!(count.load(atomic::Ordering::Relaxed), 1);
    }
}
