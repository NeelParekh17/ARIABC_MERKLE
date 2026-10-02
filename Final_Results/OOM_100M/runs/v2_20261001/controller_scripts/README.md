# Final controller configuration

These copies come from the supplied publication workspace, not a new remote read. The final V2_COMMON array matches the saved campaign.json arguments and the documented commands. The controller scripts differ from those embedded in the frozen source tarball: the orchestrator changed the load memory guard to 4608 MB and added --checksum to snapshot rsync after source freeze. Use these final orchestration settings with the frozen PG/C++/runner source; the measured runner hash matches the frozen manifest.
