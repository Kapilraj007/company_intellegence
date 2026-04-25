# Custom User Management Setup

This project now uses a custom user system backed by Supabase tables instead of Supabase Auth.

## 1. Apply the database schema

Run the SQL in `supabase/custom_user_management.sql` inside the Supabase SQL editor.

It creates:
- `public.users`
- `public.pipeline_activity_logs`
- `user_id` links on `pipeline_runs`
- `user_id` links on `agent1_raw_outputs`
- verification metadata fields on `users` (`verification_status`, `verified_at`, `verified_by`, `approval_note`)

## 2. Configure backend environment

Set these variables for the API server:

```env
SUPABASE_URL=...
SUPABASE_SERVICE_KEY=...
APP_JWT_SECRET=...
ACCESS_TOKEN_TTL_MINUTES=720
AUTH_COOKIE_SECURE=false
AUTH_COOKIE_SAMESITE=lax
```

## 3. Bootstrap the first admin

To avoid getting stuck with all accounts in `pending`, set:

```env
BOOTSTRAP_ADMIN_EMAIL=admin@example.com
BOOTSTRAP_ADMIN_PASSWORD=change-me-now
BOOTSTRAP_ADMIN_NAME=Platform Admin
```

On API startup, that account is created if missing and forced to:
- `role = admin`
- `approval_status = approved`

## 4. Runtime flow

- Signup creates a `users` row with `approval_status = pending`
- Login is blocked until an admin approves the account
- Approved logins receive a JWT session cookie
- Admins can approve/reject pending users from the profile page
- Pipeline/search/output activity is logged to `pipeline_activity_logs`
- Admins can verify users and change roles from the standalone admin panel
- Local audit logs, error logs, and per-user/company data version snapshots are stored under `output/local_store/`

## 5. Frontend behavior

- After signup, the app shows an approval-pending page
- Until approval, users cannot access pipeline/search/analytics/output features
- Authenticated API calls use the backend-managed cookie session
- Admin users land in a dedicated admin panel with:
  - user approval + verification + role controls
  - dashboard summaries (usage, pipelines, storage)
  - audit trail and error monitoring
  - data version tracking per user/company
