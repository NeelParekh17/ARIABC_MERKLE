#include "bcdb/bcdb_dsa.h"
#include "bcdb/globals.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

dsa_area    *bcdb_dsa_area;
void        *bcdb_dsa_shm;

void
attach_bcdb_dsa(void)
{
	MemoryContext oldcontext;

	if (bcdb_dsa_area != NULL)
		return;
    Assert(bcdb_dsa_shm != NULL);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    bcdb_dsa_area = dsa_attach_in_place(bcdb_dsa_shm, NULL);
	dsa_pin_mapping(bcdb_dsa_area);
	before_shmem_exit(dsa_on_shmem_exit_release_in_place,
					 PointerGetDatum(bcdb_dsa_shm));
	MemoryContextSwitchTo(oldcontext);
}

Size
bcdb_dsa_shm_size(void)
{
    return BCDB_DSA_SHM_SIZE;
}

void
create_bcdb_dsa(void)
{
    bool found;
	bcdb_dsa_shm = ShmemInitStruct("BCDB_DSA_SHM", bcdb_dsa_shm_size(), &found);
	if (!found)
	{
		dsa_area *area = dsa_create_in_place(bcdb_dsa_shm, BCDB_DSA_SHM_SIZE,
											  LWTRANCHE_BCDB_DSA, NULL);

		/* Each backend attaches its own mapping lazily for long SQL. */
		dsa_detach(area);
		on_shmem_exit(dsa_on_shmem_exit_release_in_place,
					  PointerGetDatum(bcdb_dsa_shm));
	}
}
