use futures::{FutureExt, TryStreamExt};

use crate::test::TestClient;

mod test;

#[test]
fn health() {
    let mut client = TestClient::no_destructor();

    let work = client.health().try_collect::<Vec<_>>().then(|responses| {
        async {
            for response in responses.unwrap() {
                assert_eq!(response.data.health, "true");
            }

            let ret: Result<(), ()> = Ok(());
            ret
        }
    });

    client.run(work);
}
#[test]
fn versions() {
    let mut client = TestClient::no_destructor();

    let work = client.versions().try_collect::<Vec<_>>().then(|responses| {
        async {
            for response in responses.unwrap() {
                assert_eq!(response.data.cluster_version, "2.3.0");
                assert_eq!(response.data.server_version, "2.3.8");
            }

            let ret: Result<(), ()> = Ok(());
            ret
        }
    });

    client.run(work);
}
