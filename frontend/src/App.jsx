import { useEffect, useState } from "react";
import { T } from "./theme";
import Sidebar from "./Sidebar";
import LoginPage from "./pages/LoginPage";
import DashboardPage from "./pages/DashboardPage";
import PipelinePage from "./pages/PipelinePage";
import OutputsPage from "./pages/OutputsPage";
import ExplorerPage from "./pages/ExplorerPage";
import SearchDetailPage from "./pages/SearchDetailPage";
import ProfilePage from "./pages/ProfilePage";
import InnovationClustersPage from "./pages/InnovationClustersPage";
import AnalyticsDashboardPage from "./pages/AnalyticsDashboardPage";
import ApprovalPendingPage from "./pages/ApprovalPendingPage";
import AdminPanelApp from "./admin/AdminPanelApp";
import { getCurrentUser, getHealth, logout } from "./api";

const initialSearchState = {
  mode: "companies",
  query: "fraud detection and AI risk monitoring for banks",
  excludeCompany: "",
  topK: 5,
  meta: null,
  results: [],
  selectedKey: "",
  detailKey: "",
};

const GlobalStyle = () => (
  <style>{`
	    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;500&display=swap');

	    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
	    html, body, #root { height: 100%; width: 100%; }
	    body {
	      background:
          radial-gradient(circle at top left, ${T.cyan}10, transparent 28%),
          radial-gradient(circle at top right, ${T.amber}12, transparent 30%),
          linear-gradient(180deg, ${T.navy}, ${T.navy2});
	      color: ${T.text};
	      font-family: 'DM Sans', sans-serif;
	      -webkit-font-smoothing: antialiased;
        overflow-x: hidden;
	    }
    ::-webkit-scrollbar { width: 4px; }
    ::-webkit-scrollbar-track { background: ${T.navy2}; }
    ::-webkit-scrollbar-thumb { background: ${T.border}; border-radius: 2px; }

    @keyframes fadeUp {
      from { opacity: 0; transform: translateY(16px); }
      to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulse {
      0%,100% { opacity: 1; } 50% { opacity: 0.35; }
    }
    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    /* ── Responsive layout ── */
	    .app-shell {
        display: flex;
        width: 100%;
        min-height: 100vh;
        height: 100dvh;
        overflow: hidden;
      }
	    .app-main  {
        flex: 1;
        min-width: 0;
        min-height: 0;
        padding: 36px 40px;
        overflow-y: auto;
        animation: fadeUp 0.4s ease both;
      }

	    @media (max-width: 900px) {
        .app-shell {
          flex-direction: column;
          min-height: 100dvh;
          height: auto;
          overflow: visible;
        }
	      .app-main { padding: 20px 16px; }
	    }
	    @media (max-width: 640px) {
	      .app-main { padding: 16px 12px; }
	    }
	  `}</style>
);

function LoadingScreen() {
  return (
    <div style={{
      minHeight: "100vh",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      color: T.muted,
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 12,
      letterSpacing: "0.4px",
    }}>
      Restoring session…
    </div>
  );
}

export default function App() {
  const [authReady, setAuthReady] = useState(false);
  const [user, setUser] = useState(null);
  const [authView, setAuthView] = useState("login");
  const [pendingSignup, setPendingSignup] = useState(null);
  const [page, setPage] = useState("dashboard");
  const [activePipelineId, setActivePipelineId] = useState(null);
  const [activeOutputId, setActiveOutputId] = useState(null);
  const [searchState, setSearchState] = useState(initialSearchState);
  const [adminMode, setAdminMode] = useState(true);
  const [apiInfo, setApiInfo] = useState({
    online: false,
    status: "offline",
    tasks_in_memory: 0,
    checked_at: null,
  });

  useEffect(() => {
    let mounted = true;

    const check = async () => {
      try {
        const data = await getHealth();
        if (!mounted) return;
        setApiInfo({
          online: data?.status === "ok",
          status: data?.status || "offline",
          tasks_in_memory: Number(data?.tasks_in_memory || 0),
          checked_at: new Date().toISOString(),
        });
      } catch {
        if (!mounted) return;
        setApiInfo((current) => ({
          ...current,
          online: false,
          status: "offline",
          checked_at: new Date().toISOString(),
        }));
      }
    };

    check();
    const timer = setInterval(check, 30000);
    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    let mounted = true;

    const hydrateSession = async () => {
      try {
        if (!mounted) return;
        const data = await getCurrentUser();
        setUser(data?.user || null);
      } catch {
        if (!mounted) return;
        setUser(null);
      } finally {
        if (mounted) {
          setAuthReady(true);
        }
      }
    };

    hydrateSession();

    return () => {
      mounted = false;
    };
  }, []);

  const handleLogin = (userData) => {
    setUser(userData);
    setAuthView("login");
    setPendingSignup(null);
    setAdminMode(String(userData?.role || "").toLowerCase() === "admin");
    setPage("dashboard");
  };

  const handleSignupPending = (payload) => {
    setPendingSignup(payload || null);
    setAuthView("approval-pending");
  };

  const handleLogout = async () => {
    try {
      await logout();
    } finally {
      setUser(null);
      setAuthView("login");
      setPendingSignup(null);
      setAdminMode(true);
      setPage("dashboard");
    }
  };

  if (!authReady) return (
    <>
      <GlobalStyle/>
      <LoadingScreen />
    </>
  );

  const handleNewPipeline = (taskId) => {
    setActivePipelineId(taskId);
    setPage("pipeline");
  };

  const handleOpenOutput = (outputId) => {
    setActiveOutputId(outputId);
    setPage("outputs");
  };

  const handleOpenSearchDetail = (detailKey) => {
    setSearchState((current) => ({ ...current, detailKey, selectedKey: detailKey }));
    setPage("search-detail");
  };

  const isAdmin = String(user?.role || "").toLowerCase() === "admin";

  if (!user) return (
    <>
      <GlobalStyle/>
      {authView === "approval-pending"
        ? (
          <ApprovalPendingPage
            signupInfo={pendingSignup}
            onBackToLogin={() => {
              setPendingSignup(null);
              setAuthView("login");
            }}
          />
        )
        : (
          <LoginPage
            onLogin={handleLogin}
            onSignupPending={handleSignupPending}
          />
        )}
    </>
  );

  if (isAdmin && adminMode) {
    return (
      <AdminPanelApp
        user={user}
        onLogout={handleLogout}
        onOpenWorkspace={() => {
          setAdminMode(false);
          setPage("dashboard");
        }}
      />
    );
  }

  const pages = {
    dashboard: (
      <DashboardPage
        setPage={setPage}
        setActivePipeline={handleNewPipeline}
        setActiveOutput={handleOpenOutput}
        apiInfo={apiInfo}
      />
    ),
    pipeline: (
      <PipelinePage
        key={activePipelineId || "pipeline"}
        activePipelineId={activePipelineId}
        setActiveOutput={handleOpenOutput}
      />
    ),
    explorer: (
      <ExplorerPage
        apiInfo={apiInfo}
        setActivePipeline={handleNewPipeline}
        searchState={searchState}
        setSearchState={setSearchState}
        openSearchDetail={handleOpenSearchDetail}
      />
    ),
    "search-detail": (
      <SearchDetailPage
        apiInfo={apiInfo}
        setPage={setPage}
        setActivePipeline={handleNewPipeline}
        searchState={searchState}
        setSearchState={setSearchState}
        openSearchDetail={handleOpenSearchDetail}
      />
    ),
    clusters: <InnovationClustersPage apiInfo={apiInfo} />,
    analytics: <AnalyticsDashboardPage apiInfo={apiInfo} />,
    outputs: <OutputsPage activeOutputId={activeOutputId}/>,
    profile: (
      <ProfilePage
        user={{ ...user, apiOnline: apiInfo.online }}
        apiInfo={apiInfo}
        onLogout={handleLogout}
      />
    ),
  };

  return (
    <>
      <GlobalStyle/>
      <div className="app-shell">
        {isAdmin && !adminMode && (
          <button
            onClick={() => setAdminMode(true)}
            style={{
              position: "fixed",
              right: 16,
              bottom: 16,
              zIndex: 50,
              border: `1px solid ${T.border}`,
              background: T.navy2,
              color: T.text,
              borderRadius: 10,
              padding: "10px 12px",
              cursor: "pointer",
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 11,
              letterSpacing: "0.4px",
            }}
          >
            Open Admin Panel
          </button>
        )}
        <Sidebar page={page} setPage={setPage} user={user} apiInfo={apiInfo}/>
        <main className="app-main">
          {pages[page] || pages.dashboard}
        </main>
      </div>
    </>
  );
}
