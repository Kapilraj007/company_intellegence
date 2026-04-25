import { useState } from "react";
import { T } from "../theme";
import { Card, Btn, Input, Spinner } from "../components";
import { Logo } from "../components";
import { healthCheck, login, signup } from "../api";

function getAuthErrorMessage(error, mode) {
  const message = error?.message || "Authentication failed.";
  const lower = message.toLowerCase();

  if (lower.includes("invalid email or password")) {
    return "Invalid email or password.";
  }

  if (lower.includes("pending admin approval")) {
    return "Your account is waiting for admin approval.";
  }

  if (lower.includes("rejected")) {
    return "Your access request was rejected. Please contact an administrator.";
  }

  if (mode === "register" && lower.includes("already exists")) {
    return "An account with this email already exists. Try signing in instead.";
  }

  return message;
}

export default function LoginPage({ onLogin, onSignupPending }) {
  const [mode, setMode] = useState("sign-in");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [pass, setPass] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [note, setNote] = useState("");

  const handle = async () => {
    if (!email || !pass || (mode === "register" && !name.trim())) {
      setErr("Please fill in all required fields.");
      return;
    }

    setLoading(true);
    setErr("");
    setNote("");

    try {
      if (mode === "register") {
        const data = await signup({ name: name.trim(), email, password: pass });
        onSignupPending?.(data?.user || { name: name.trim(), email });
        setNote(data?.message || "Request has been sent to admin for approval.");
        return;
      }

      const data = await login({ email, password: pass });
      if (data?.user) {
        const apiOnline = await healthCheck();
        onLogin({
          ...data.user,
          apiOnline,
        });
        return;
      }
    } catch (error) {
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
    ? "Request access to the research workspace"
    : "Access your approved workspace";

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
            {mode === "register" && (
              <Input label="Name" type="text" value={name}
                onChange={e => setName(e.target.value)}
                placeholder="Kapil Sharma" icon="👤"
                onKeyDown={handleKey}
              />
            )}
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

            <Btn onClick={handle} disabled={loading} full style={{ marginTop: 4, padding: "13px 22px", fontSize: 14 }}>
              {loading
                ? <><Spinner size={14} color="#000"/> Authenticating…</>
                : mode === "register" ? "Request Access →" : "Sign In →"}
            </Btn>
          </div>

          <div style={{
            marginTop: 20, padding: "12px 14px", background: T.navy3,
            borderRadius: 8, fontSize: 12, color: T.muted,
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            {mode === "register"
              ? "New accounts stay pending until an admin approves access."
              : "Approved users receive a secure server-managed session."}
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
