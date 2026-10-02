# M09 review fix verification

## M02 fixture requests (read-only consumer; no backend edits)

Please add shared paged-overview examples for clean, external_ahead and desired_ahead,
and an ambiguous-create operation/evidence example. Existing M02 binding_status.json
covers all eight drift enums; the frontend consumes that file directly rather than
inventing a shared fixture. No shared fixture was authored or modified by M09.

Approval projections and validate evidence are optional server response seams. If a real
backend does not supply them, the browser displays evidence unavailable and blocks the
approval request. Demo-only projections are supplied by the injected fake server.
