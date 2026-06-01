/// Send a oneshot request message to an actor and await the reply.
///
/// Usage:
/// `let reply = actor_ask!(actor_ref, |reply| Msg::Get(reply))?;`
#[macro_export]
macro_rules! actor_ask {
    ($actor:expr, |$reply:ident| $msg:expr) => {{
        let ($reply, __rx) = tokio::sync::oneshot::channel();
        match $actor.send($msg).await {
            Ok(()) => __rx
                .await
                .map_err(|_| -> $crate::actor::ActorProcessingErr { "actor dropped reply".into() }),
            Err(e) => Err(e),
        }
    }};
}

/// Build a named child spec for a shared [`ChildRegistry`] with less closure boilerplate.
///
/// Usage:
/// `registry_child_spec!(0, "calculator", registry, Calculator { ... })`
#[macro_export]
macro_rules! registry_child_spec {
    ($order:expr, $name:expr, $registry:expr, $actor_expr:expr) => {{
        let __registry = $registry.clone();
        lane_switchboards::supervisor::spawn_child_spec($order, $name, __registry, {
            let __registry = $registry.clone();
            move || {
                let _ = &__registry;
                $actor_expr
            }
        })
    }};
}
