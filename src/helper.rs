use axum::http::HeaderMap;

pub(super) fn set_headers(headers: &HeaderMap, lua_headers: &mlua::Table) -> mlua::Result<()> {
    for (key, value) in headers.iter() {
        if let Ok(value) = value.to_str() {
            lua_headers.set(key.as_str(), value)?;
        }
    }

    Ok(())
}
