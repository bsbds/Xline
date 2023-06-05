use xline_client::{
    clients::auth::{
        AuthClient, AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGetRequest,
        AuthRoleGrantPermissionRequest, AuthRoleRevokePermissionRequest, AuthUserAddRequest,
        AuthUserChangePasswordRequest, AuthUserDeleteRequest, AuthUserGetRequest,
        AuthUserGrantRoleRequest, AuthUserRevokeRoleRequest, Permission,
    },
    error::Result,
    Client, ClientOptions,
};
use xline_test_utils::Cluster;
use xlineapi::Type;

#[tokio::test]
async fn role() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await?;

    let role1 = "role1";
    let role2 = "role2";

    let _ = client.role_delete(AuthRoleDeleteRequest::new(role1)).await;
    let _ = client.role_delete(AuthRoleDeleteRequest::new(role2)).await;

    client.role_add(AuthRoleAddRequest::new(role1)).await?;

    client.role_get(AuthRoleGetRequest::new(role1)).await?;

    client
        .role_delete(AuthRoleDeleteRequest::new(role1))
        .await?;
    client
        .role_get(AuthRoleGetRequest::new(role1))
        .await
        .unwrap_err();

    client.role_add(AuthRoleAddRequest::new(role2)).await?;
    client.role_get(AuthRoleGetRequest::new(role2)).await?;

    {
        let resp = client.role_list().await?;
        assert!(resp.roles.contains(&role2.to_string()));
    }

    let perm0 = Permission::new(Type::Read, "123");
    let perm1 = Permission::new(Type::Write, "abc").with_from_key();
    let perm2 = Permission::new(Type::Readwrite, "hi").with_range_end("hjj");
    let perm3 = Permission::new(Type::Write, "pp").with_prefix();
    let perm4 = Permission::new(Type::Read, vec![0]).with_from_key();

    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role2, perm0.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role2, perm1.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role2, perm2.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role2, perm3.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role2, perm4.clone()))
        .await?;

    {
        let resp = client.role_get(AuthRoleGetRequest::new(role2)).await?;
        let permissions = resp.perm;
        assert!(permissions.contains(&perm0.into()));
        assert!(permissions.contains(&perm1.into()));
        assert!(permissions.contains(&perm2.into()));
        assert!(permissions.contains(&perm3.into()));
        assert!(permissions.contains(&perm4.into()));
    }

    // revoke all permission
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role2, "123"))
        .await?;
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role2, "abc").with_from_key())
        .await?;
    client
        .role_revoke_permission(
            AuthRoleRevokePermissionRequest::new(role2, "hi").with_range_end("hjj"),
        )
        .await?;
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role2, "pp").with_prefix())
        .await?;
    client
        .role_revoke_permission(
            AuthRoleRevokePermissionRequest::new(role2, vec![0]).with_from_key(),
        )
        .await?;

    let resp = client.role_get(AuthRoleGetRequest::new(role2)).await?;
    assert!(resp.perm.is_empty());

    client
        .role_delete(AuthRoleDeleteRequest::new(role2))
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_user() -> Result<()> {
    let name1 = "usr1";
    let password1 = "pwd1";
    let name2 = "usr2";
    let password2 = "pwd2";
    let name3 = "usr3";
    let password3 = "pwd3";
    let role1 = "role1";

    let (_cluster, client) = get_cluster_client().await?;

    // ignore result
    let _resp = client.user_delete(AuthUserDeleteRequest::new(name1)).await;
    let _resp = client.user_delete(AuthUserDeleteRequest::new(name2)).await;
    let _resp = client.user_delete(AuthUserDeleteRequest::new(name3)).await;
    let _resp = client.role_delete(AuthRoleDeleteRequest::new(role1)).await;

    client
        .user_add(AuthUserAddRequest::new(name1).with_pwd(password1))
        .await?;

    client
        .user_add(AuthUserAddRequest::new(name2).with_no_pwd())
        .await?;

    client
        .user_add(AuthUserAddRequest::new(name3).with_pwd(password3))
        .await?;

    client.user_get(AuthUserGetRequest::new(name1)).await?;

    {
        let resp = client.user_list().await?;
        assert!(resp.users.contains(&name1.to_string()));
    }

    client
        .user_delete(AuthUserDeleteRequest::new(name2))
        .await?;
    client
        .user_get(AuthUserGetRequest::new(name2))
        .await
        .unwrap_err();

    client
        .user_change_password(AuthUserChangePasswordRequest::new(name1, password2))
        .await?;
    client.user_get(AuthUserGetRequest::new(name1)).await?;

    client.role_add(AuthRoleAddRequest::new(role1)).await?;
    client
        .user_grant_role(AuthUserGrantRoleRequest::new(name1, role1))
        .await?;
    client.user_get(AuthUserGetRequest::new(name1)).await?;

    client
        .user_revoke_role(AuthUserRevokeRoleRequest::new(name1, role1))
        .await?;
    client.user_get(AuthUserGetRequest::new(name1)).await?;

    let _ = client.user_delete(AuthUserDeleteRequest::new(name1)).await;
    let _ = client.user_delete(AuthUserDeleteRequest::new(name2)).await;
    let _ = client.user_delete(AuthUserDeleteRequest::new(name3)).await;
    let _ = client.role_delete(AuthRoleDeleteRequest::new(role1)).await;

    Ok(())
}

async fn get_cluster_client() -> Result<(Cluster, AuthClient)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs().clone(), ClientOptions::default())
        .await?
        .auth_client();
    Ok((cluster, client))
}
