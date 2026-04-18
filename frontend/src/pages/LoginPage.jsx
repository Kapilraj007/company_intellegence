import { useState } from "react";
import { T } from "../theme";
import { Card, Btn, Input, Spinner } from "../components";
import { Logo } from "../components";
import { healthCheck } from "../api";
import {
  isSupabaseConfigured,
  mapAuthUser,
  signInWithPassword,
  signUpWithPassword,
} from "../supabaseClient";

function getAuthErrorMessage(error, mode) {
  const message = error?.message || "Authentication failed.";
  const lower = message.toLowerCase();

  if (lower.includes("invalid login credentials")) {
    return "Invalid email or password.";
  }

  if (lower.includes("email not confirmed")) {
    return "Check your inbox to confirm your email, then sign in.";
  }

  if (mode === "register" && lower.includes("already registered")) {
    return "An account with this email already exists. Try signing in instead.";
  }

  return message;
}

export default function LoginPage({ onLogin }) {
  const [mode, setMode]         = useState("sign-in");
  const [email, setEmail]     = useState("");
  const [pass,  setPass]      = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr]         = useState("");
  const [note, setNote]       = useState("");
  const configured = isSupabaseConfigured();

  const handle = async () => {
    console.log("[LoginPage] Button clicked. Mode:", mode, "Email:", email);
    
    if (!email || !pass) { 
      setErr("Please fill in all fields."); 
      console.log("[LoginPage] Validation failed: missing email or password");
      return; 
    }
    
    if (!configured) {
      const errMsg = "Supabase auth is not configured. Set VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY.";
      setErr(errMsg);
      console.error("[LoginPage]", errMsg);
      return;
    }

    setLoading(true); setErr("");
    setNote("");

    try {
      const action = mode === "register" ? signUpWithPassword : signInWithPassword;
      console.log("[LoginPage] Calling auth action:", mode === "register" ? "signUpWithPassword" : "signInWithPassword");
      
      const { data, error } = await action({ email, password: pass });
      console.log("[LoginPage] Auth response - Data:", data, "Error:", error);
      
      if (error) {
        console.error("[LoginPage] Auth error:", error);
        throw error;
      }

      const signedInUser = data?.session?.user || null;
      console.log("[LoginPage] Signed-in user:", signedInUser);
      
      if (signedInUser) {
        const apiOnline = await healthCheck();
        console.log("[LoginPage] Health check result:", apiOnline);
        onLogin({
          ...mapAuthUser(signedInUser),
          apiOnline,
        });
        return;
      }

      if (mode === "register" && data?.user) {
        console.log("[LoginPage] Account created (email confirmation required)");
        setMode("sign-in");
        setNote("Account created. If email confirmation is enabled, confirm your inbox and then sign in.");
      }
    } catch (error) {
      console.error("[LoginPage] Exception:", error);
      setErr(getAuthErrorMessage(error, mode));
    } finally {
      setLoading(false);
    }
  };

  const toggleMode = () => {
    setMode((current) => current === "sign-in" ? "register" : "sign-in");
    setErr("");
    setNote("");
  };

  const handleKey = e => { if (e.key === "Enter") handle(); };
  const title = mode === "register" ? "Create account" : "Sign in";
  const subtitle = mode === "register"
    ? "Create a secure workspace account"
    : "Access your research workspace";

  return (
    <div style={{
      minHeight: "100vh", background: T.navy,
      display: "flex", alignItems: "center", justifyContent: "center",
      position: "relative", overflow: "hidden",
    }}>
      {/* Grid background */}
      <div style={{
        position: "absolute", inset: 0, opacity: 0.04,
        backgroundImage: `linear-gradient(${T.cyan} 1px, transparent 1px), linear-gradient(90deg, ${T.cyan} 1px, transparent 1px)`,
        backgroundSize: "60px 60px",
      }}/>
      {/* Glow orbs */}
      <div style={{ position: "absolute", top: "20%", left: "15%", width: 300, height: 300, borderRadius: "50%", background: T.amber + "08", filter: "blur(80px)" }}/>
      <div style={{ position: "absolute", bottom: "20%", right: "15%", width: 250, height: 250, borderRadius: "50%", background: T.cyan + "06", filter: "blur(80px)" }}/>

      <div style={{ width: "100%", maxWidth: 420, padding: "0 20px", animation: "fadeUp 0.5s ease both" }}>
        <div style={{ textAlign: "center", marginBottom: 40 }}>
          <Logo size={26} />
          <p style={{ marginTop: 12, fontSize: 14, color: T.muted }}>Company Intelligence Platform</p>
        </div>

        <Card style={{ padding: 36 }}>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 22, color: T.white, marginBottom: 6 }}>
            {title}
          </h2>
          <p style={{ fontSize: 13, color: T.muted, marginBottom: 28 }}>{subtitle}</p>

          <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <Input label="Email" type="email" value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="analyst@firm.com" icon="✉"
              onKeyDown={handleKey}
            />
            <Input label="Password" type="password" value={pass}
              onChange={e => setPass(e.target.value)}
              placeholder="••••••••" icon="🔒"
              onKeyDown={handleKey}
            />

            {err && (
              <div style={{
                background: T.red + "15", border: `1px solid ${T.red}30`,
                borderRadius: 8, padding: "10px 14px", fontSize: 13, color: T.red,
              }}>
                {err}
              </div>
            )}

            {note && (
              <div style={{
                background: T.cyan + "12", border: `1px solid ${T.cyan}28`,
                borderRadius: 8, padding: "10px 14px", fontSize: 13, color: T.cyan,
              }}>
                {note}
              </div>
            )}

            {!configured && (
              <div style={{
                background: T.amber + "15", border: `1px solid ${T.amber}40`,
                borderRadius: 8, padding: "10px 14px", fontSize: 12, color: T.amber,
                fontFamily: "'JetBrains Mono', monospace",
              }}>
                <strong>⚠️ Missing Environment:</strong> You need to set VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY in your .env file.
                <div style={{ marginTop: 8, fontSize: 11, opacity: 0.9 }}>
                  📋 Get your keys from Supabase: Settings → API → "anon" key
                </div>
              </div>
            )}

            <Btn onClick={handle} disabled={loading} full style={{ marginTop: 4, padding: "13px 22px", fontSize: 14 }}>
              {loading
                ? <><Spinner size={14} color="#000"/> Authenticating…</>
                : mode === "register" ? "Create Account →" : "Sign In →"}
            </Btn>
          </div>

          <div style={{
            marginTop: 20, padding: "12px 14px", background: T.navy3,
            borderRadius: 8, fontSize: 12, color: T.muted,
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            {mode === "register"
              ? "Create a Supabase-backed email/password account."
              : "Use your Supabase email/password account to enter the workspace."}
          </div>

          <div style={{ marginTop: 14, display: "flex", justifyContent: "center", gap: 8, fontSize: 12, color: T.muted }}>
            <span>{mode === "register" ? "Already registered?" : "Need an account?"}</span>
            <button
              type="button"
              onClick={toggleMode}
              style={{
                border: "none",
                background: "transparent",
                color: T.amber,
                cursor: "pointer",
                fontFamily: "'Syne', sans-serif",
                fontSize: 12,
                fontWeight: 700,
              }}
            >
              {mode === "register" ? "Sign in" : "Create one"}
            </button>
          </div>
        </Card>
      </div>
    </div>
  );
}
