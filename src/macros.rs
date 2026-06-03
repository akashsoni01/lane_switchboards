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

/// Lookup a child in [`ChildRegistry`] and perform an `actor_ask!` in one step.
///
/// Usage:
/// `let reply = registry_ask!(registry, ChildName::Calculator, "calculator not running", |reply| Msg::Get(reply))?;`
#[macro_export]
macro_rules! registry_ask {
    ($registry:expr, $name:expr, $missing:expr, |$reply:ident| $msg:expr) => {{
        let __name = $name;
        match $registry.get(&__name).await {
            Some(__actor) => $crate::actor_ask!(__actor, |$reply| $msg),
            None => Err::<_, $crate::actor::ActorProcessingErr>($missing.into()),
        }
    }};
}

/// Start a one-child supervisor with a [`ChildRegistry`] entry (see [`supervise_named_child`]).
///
/// ```ignore
/// supervise_named_child!("dao-a", registry, config, DaoAActor { ... }).await?;
/// supervise_named_child!("dao-a", registry, config, Duration::from_millis(50), DaoAActor { ... }).await?;
/// ```
#[macro_export]
macro_rules! supervise_named_child {
    ($name:expr, $registry:expr, $config:expr, $settle:expr, $actor:expr) => {{
        let __registry = $registry;
        $crate::supervisor::supervise_named_child_settled(
            $name,
            __registry,
            $config,
            $settle,
            move || $actor,
        )
    }};
    ($name:expr, $registry:expr, $config:expr, $actor:expr) => {{
        let __registry = $registry;
        $crate::supervisor::supervise_named_child($name, __registry, $config, move || $actor)
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
