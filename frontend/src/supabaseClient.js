import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = (import.meta.env.VITE_SUPABASE_URL || "").trim();
const SUPABASE_ANON_KEY = (import.meta.env.VITE_SUPABASE_ANON_KEY || "").trim();
const AUTH_STORAGE_KEY = "company-intelligence-auth";

const CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);

let supabaseClient = null;

if (CONFIGURED) {
  supabaseClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    auth: {
      persistSession: true,
      autoRefreshToken: true,
      detectSessionInUrl: true,
      storageKey: AUTH_STORAGE_KEY,
    },
  });
}

export function isSupabaseConfigured() {
  if (!CONFIGURED) {
    console.warn("[Supabase] Not configured. VITE_SUPABASE_URL:", SUPABASE_URL ? "✓" : "✗", "VITE_SUPABASE_ANON_KEY:", SUPABASE_ANON_KEY ? "✓" : "✗");
  }
  return CONFIGURED;
}

export function getSupabaseClient() {
  if (!supabaseClient) {
    throw new Error("Supabase auth is not configured. Set VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY.");
  }

  return supabaseClient;
}

export function mapAuthUser(user) {
  if (!user) {
    return null;
  }

  const email = user.email || "";
  const displayName =
    user.user_metadata?.display_name ||
    user.user_metadata?.name ||
    email.split("@")[0] ||
    "User";

  return {
    id: user.id,
    name: displayName,
    email,
    role: "Analyst",
  };
}

export async function getSession() {
  return getSupabaseClient().auth.getSession();
}

export function onAuthStateChange(callback) {
  return getSupabaseClient().auth.onAuthStateChange(callback);
}

export async function signInWithPassword({ email, password }) {
  return getSupabaseClient().auth.signInWithPassword({ email, password });
}

export async function signUpWithPassword({ email, password }) {
  console.log("[Supabase] signUpWithPassword called for:", email);
  try {
    const response = await getSupabaseClient().auth.signUp({
      email,
      password,
      options: {
        data: {
          display_name: email.split("@")[0] || "User",
        },
      },
    });
    console.log("[Supabase] signUpWithPassword response:", response);
    return response;
  } catch (error) {
    console.error("[Supabase] signUpWithPassword error:", error);
    throw error;
  }
}

export async function signOut() {
  return getSupabaseClient().auth.signOut();
}

export async function refreshAccessToken() {
  const { data, error } = await getSupabaseClient().auth.refreshSession();
  if (error) {
    throw error;
  }
  return data.session?.access_token || "";
}

export async function getAccessToken() {
  const { data, error } = await getSession();

  if (error) {
    throw error;
  }

  return data.session?.access_token || "";
}
