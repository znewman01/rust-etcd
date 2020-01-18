use etcd::stats;
use futures::TryStreamExt;

use crate::test::TestClient;

mod test;

#[test]
fn leader_stats() {
    let mut client = TestClient::no_destructor();

    let work = stats::leader_stats(&client);

    client.run(work);
}

#[test]
fn self_stats() {
    let mut client = TestClient::no_destructor();

    let work = stats::self_stats(&client).try_collect::<Vec<_>>(); // Future<Output = Result<Vec<_>, _>>

    client.run(work);
}

#[test]
fn store_stats() {
    let mut client = TestClient::no_destructor();

    let work = stats::store_stats(&client).try_collect::<Vec<_>>();

    client.run(work);
}
