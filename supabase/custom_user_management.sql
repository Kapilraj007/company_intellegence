create extension if not exists pgcrypto;

create table if not exists public.users (
    user_id uuid primary key default gen_random_uuid(),
    name text not null,
    email text not null,
    password text not null,
    role text not null default 'user' check (role in ('user', 'admin')),
    approval_status text not null default 'pending' check (approval_status in ('pending', 'approved', 'rejected')),
    session_nonce text not null default encode(gen_random_bytes(32), 'hex'),
    last_login_at timestamptz,
    created_at timestamptz not null default timezone('utc', now())
);

create unique index if not exists users_email_unique_idx on public.users (lower(email));

alter table if exists public.users
    add column if not exists verification_status text not null default 'unverified'
    check (verification_status in ('unverified', 'verified'));

alter table if exists public.users
    add column if not exists verified_at timestamptz;

alter table if exists public.users
    add column if not exists verified_by uuid references public.users(user_id);

alter table if exists public.users
    add column if not exists approval_note text;

create table if not exists public.pipeline_activity_logs (
    activity_id uuid primary key default gen_random_uuid(),
    user_id uuid not null references public.users(user_id) on delete cascade,
    run_id text,
    company_id text,
    company_name text,
    activity_type text not null,
    activity_status text,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now())
);

create index if not exists pipeline_activity_logs_user_id_idx on public.pipeline_activity_logs (user_id, created_at desc);
create index if not exists pipeline_activity_logs_run_id_idx on public.pipeline_activity_logs (run_id);
create index if not exists pipeline_activity_logs_activity_type_idx on public.pipeline_activity_logs (activity_type, created_at desc);

alter table if exists public.pipeline_runs
    add column if not exists user_id uuid references public.users(user_id);

alter table if exists public.agent1_raw_outputs
    add column if not exists user_id uuid references public.users(user_id);

create index if not exists pipeline_runs_user_id_idx on public.pipeline_runs (user_id);
create index if not exists agent1_raw_outputs_user_id_idx on public.agent1_raw_outputs (user_id);
