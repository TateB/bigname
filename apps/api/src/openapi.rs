mod openapi {
    mod responses {
        include!("openapi/responses.rs");
    }

    mod route_operations {
        include!("openapi/route_operations.rs");
    }

    mod schemas {
        include!("openapi/schemas.rs");
    }

    mod server {
        include!("openapi/server.rs");
    }

    #[cfg(test)]
    pub(super) use server::{app_router, openapi_document};
    pub(super) use server::{render_openapi_document, serve};
}

#[cfg(test)]
use openapi::{app_router, openapi_document};
use openapi::{render_openapi_document, serve};
