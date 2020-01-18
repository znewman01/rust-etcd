use etcd::auth::{self, AuthChange, NewUser, Role, RoleUpdate, UserUpdate};
use etcd::{BasicAuth, Client};
use tokio::runtime::Runtime;

#[test]
fn auth() {
    let client = Client::new(&["http://etcd:2379"], None).unwrap();

    let basic_auth = BasicAuth {
        username: "root".into(),
        password: "secret".into(),
    };

    let authed_client = Client::new(&["http://etcd:2379"], Some(basic_auth)).unwrap();

    let root_user = NewUser::new("root", "secret");

    let work = async {
        let response = auth::status(&client).await.unwrap();
        assert_eq!(response.data, false);

        let response = auth::create_user(&client, root_user).await.unwrap();
        assert_eq!(response.data.name(), "root");

        let response = auth::enable(&client).await.unwrap();
        assert_eq!(response.data, AuthChange::Changed);

        let mut update_guest = RoleUpdate::new("guest");
        update_guest.revoke_kv_write_permission("/*");
        auth::update_role(&authed_client, update_guest)
            .await
            .unwrap();

        let mut rkt_role = Role::new("rkt");
        rkt_role.grant_kv_read_permission("/rkt/*");
        rkt_role.grant_kv_write_permission("/rkt/*");
        auth::create_role(&authed_client, rkt_role).await.unwrap();

        let mut rkt_user = NewUser::new("rkt", "secret");
        rkt_user.add_role("rkt");
        let response = auth::create_user(&authed_client, rkt_user).await.unwrap();
        let rkt_user = response.data;
        assert_eq!(rkt_user.name(), "rkt");
        let role_name = &rkt_user.role_names()[0];
        assert_eq!(role_name, "rkt");

        let mut update_rkt_user = UserUpdate::new("rkt");
        update_rkt_user.update_password("secret2");
        update_rkt_user.grant_role("root");
        auth::update_user(&authed_client, update_rkt_user)
            .await
            .unwrap();

        let response = auth::get_role(&authed_client, "rkt").await.unwrap();
        let role = response.data;
        assert!(role.kv_read_permissions().contains(&"/rkt/*".to_owned()));
        assert!(role.kv_write_permissions().contains(&"/rkt/*".to_owned()));

        auth::delete_user(&authed_client, "rkt").await.unwrap();

        auth::delete_role(&authed_client, "rkt").await.unwrap();

        let mut update_guest = RoleUpdate::new("guest");
        update_guest.grant_kv_write_permission("/*");
        auth::update_role(&authed_client, update_guest)
            .await
            .unwrap();

        let response = auth::disable(&authed_client).await.unwrap();
        assert_eq!(response.data, AuthChange::Changed);

        let ret: Result<(), Vec<etcd::Error>> = Ok(());
        ret
    };

    let _ = Runtime::new()
        .expect("failed to create Tokio runtime")
        .block_on(work);
}
