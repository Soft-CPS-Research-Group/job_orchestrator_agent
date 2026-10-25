# 1.5.0 — final catalog boundary

Remove the Training URL proxy, dataset/configuration catalog routes, duplicate Mongo generator and its scientific/Mongo dependencies from Orchestrator. Catalog requests now belong only to authenticated Training workspaces. Keep execution input snapshots, resolved configurations, worker control, historical job access and result retrieval. No real experiment file or SQL job is removed.

The old catalog URLs return 404. This intentionally ends their compatibility period; the deployed UI already calls Training directly. Tests verify the absence of old routes and preserve job execution coverage. Generation remains tested in Training.
