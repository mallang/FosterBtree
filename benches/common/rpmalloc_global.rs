// Keep the allocator fixed across experiment binaries so local/server
// comparisons are not confounded by platform-default allocator differences.
#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;
