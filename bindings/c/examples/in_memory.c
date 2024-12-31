#include "assert.h"
#include "slatedb.h"
#include "stdio.h"
#include <string.h>

int main()
{
    const char *path = "/tmp/test";
    slatedb_config_source cfg = {
        .tag = FROM_FILE,
        .from_file = "./config.json",
    };
    slatedb_object_store obj = {.tag = IN_MEMORY};
    slatedb_new_result result = slatedb_instance_new(path, cfg, obj);
    if (result.error != NULL)
    {
        printf("Error creating DB: %s\n", result.error->message);
        return 1;
    }
    printf("config set\n");
    assert(result.instance != NULL);
    assert(result.error == NULL);
    printf("created instance\n");

    slatedb_instance *instance = result.instance;
    slatedb_error *put_error = slatedb_instance_put(instance, "key1", "value1");
    assert(put_error == NULL);
    printf("put success\n");

    slatedb_error *flush_err = slatedb_instance_flush(instance);
    assert(flush_err == NULL);
    printf("flush success\n");

    slatedb_instance_get_result get_result = slatedb_instance_get(instance, "key1");
    if (get_result.error != NULL)
    {
        printf("Error creating DB: %s\n", result.error->message);
        return 1;
    }
    assert(get_result.error == NULL);
    assert(strcmp(get_result.value, "value1") == 0);
    printf("retrieved val: %s\n", get_result.value);

    slatedb_error *err = slatedb_instance_close(instance);
    assert(err == NULL);
    printf("closed instance\n");
    slatedb_instance_free(instance);
}
