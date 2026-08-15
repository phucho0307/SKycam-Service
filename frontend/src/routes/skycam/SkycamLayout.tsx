import { NavLink, Outlet } from "react-router-dom";

const nav = [
  { to: "/camera", label: "Live View", end: true },
  { to: "/camera/images", label: "Images", end: false },
  { to: "/camera/telemetry", label: "Telemetry", end: false },
  { to: "/camera/settings", label: "Settings", end: false },
];

export default function SkycamLayout() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Sky Camera</h1>
          <p className="text-sm text-slate-400">Observatory Services · device “skycam”</p>
        </div>
        <span className="inline-flex items-center gap-2 rounded-full border border-emerald-800 bg-emerald-950/50 px-3 py-1 text-xs text-emerald-300">
          <span className="h-2 w-2 rounded-full bg-emerald-400" />
          online
        </span>
      </div>

      <div className="grid grid-cols-1 gap-6 md:grid-cols-[200px_1fr]">
        <aside>
          <nav className="flex flex-col gap-1 rounded-xl border border-slate-800 bg-slate-900/40 p-2">
            {nav.map((n) => (
              <NavLink
                key={n.to}
                to={n.to}
                end={n.end}
                className={({ isActive }) =>
                  [
                    "rounded-lg px-3 py-2 text-sm transition-colors",
                    isActive
                      ? "bg-brand-500/15 text-white"
                      : "text-slate-300 hover:bg-slate-800/60 hover:text-white",
                  ].join(" ")
                }
              >
                {n.label}
              </NavLink>
            ))}
          </nav>
        </aside>

        <section className="min-w-0">
          <Outlet />
        </section>
      </div>
    </div>
  );
}
