use etcd::members;
use futures::future::TryFutureExt;

use crate::test::TestClient;

mod test;

#[test]
fn list() {
    let mut client = TestClient::no_destructor();

    let work = members::list(&client).and_then(|res| async {
        let members = res.data;
        let member = &members[0];

        assert_eq!(member.name, "default");

        Ok(())
    });

    client.run(work);
}
