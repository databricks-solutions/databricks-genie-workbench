# MV-advisor mockups — review scaffold (Prompt 10)

Static mockups for the metric-view advisor UI, generated for the Prompt 10
design review. Every screen is exported in **both light and dark** because the
panels carry `dark:` variants and a single-theme export reviews half the states.
Open `index.html` in a browser — no server, no network.

Copy is verbatim from POV §7.3 / §7.5 and the Prompt 10 review, except where
**authored new** and flagged for explicit sign-off: **frame 7b** (empty-state
copy — see the header of `MvIqScanAdvisoryMockups.tsx`) and **frame 8**'s input
label/helper and both refused-variant messages (see the header of
`MvByoRegistrationMockups.tsx`; the verified "Registered. …" sentence there is
verbatim from the review).

## Frames

| # | Screen |
|---|--------|
| 1 | Run config — first run ("Create and attach" disabled, MV-D1) |
| 2 | Run config — re-run (approved *for this Agent*, granted probe) |
| 3 | Run config — denial |
| 4 | Output — suggest only (verbatim "Lift not measured…"; card 1 has no join-strategy ladder, card 2 keeps *Subquery source* — reachable states only, MV-D14/D15) |
| 5 | Output — create and attach (DETACHED regression + Drop view; provenance `OBO_CREATED`) |
| 6 | Semantic model (static preview; live graph is Prompt 12) |
| 7a / 7b / 7c | IQ Scan advisory — found / empty / not entitled |
| 8a / 8b / 8c | BYO registration (MV-D24) — entry points / verified (`USER_CREATED`, no Drop) / refused (not a metric view · not visible) |

## Regenerate

Components live in `frontend/src/components/auto-optimize/mockups/` (so vitest
collects `mockups.test.tsx`). The emitter lives in `frontend/scripts/mockups/`.
From `frontend/`:

```bash
npx vite build -c scripts/mockups/vite.css.config.mts   # index.css -> .dist-css/mockups.css
npx vite build -c scripts/mockups/vite.ssr.config.mts    # bundle emit.tsx
node scripts/mockups/.dist/emit.mjs                       # write *.html + mockups.css here
```

## Disposal — these files are temporary

This is a **review scaffold, not production code**, and there is no
visual-regression harness that would give it a second life. Delete it as the
real panels land:

- **Prompt 11** implements run-config frames 1–3 → delete `MvRunConfigMockups.tsx`.
- **Prompt 13** implements output frames 4–6 → delete `MvOutputMockups.tsx`,
  `MvSemanticModelFrame.tsx`.
- **Prompt 13.5** implements IQ-Scan frame 7 and BYO registration frame 8
  (MV-D24) → delete `MvIqScanAdvisoryMockups.tsx` and `MvByoRegistrationMockups.tsx`,
  and graduate or delete `MvProposalCard.tsx`.

When the last frame is gone, remove `frontend/src/components/auto-optimize/mockups/`,
`frontend/scripts/mockups/`, and this `docs/design/mockups/` directory in the same
commit. Nothing outside the scaffold imports it.
