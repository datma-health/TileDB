// Function Pointers for blosc
#ifdef BLOSC_IMPL
    #undef BLOSC_DLL_EXPORT
        #define BLOSC_DLL_EXPORT __declspec(dllexport)
#else
    #undef BLOSC_DLL_EXPORT
        #define BLOSC_DLL_EXPORT extern
#endif

BLOSC_DLL_EXPORT void (*blosc_init)();
BLOSC_DLL_EXPORT void (*blosc_destroy)();
BLOSC_DLL_EXPORT int (*blosc_set_compressor)(const char *);
BLOSC_DLL_EXPORT int (*blosc_compress)(int, int, size_t, size_t, const void *, void *, size_t);
BLOSC_DLL_EXPORT int (*blosc_decompress)(const void *, void *, size_t)
