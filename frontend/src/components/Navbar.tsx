import { NavLink } from "react-router-dom";

const links = [
  { to: "/hardware", label: "Hardware" },
  { to: "/software", label: "Software" },
  { to: "/imaging", label: "Imaging" },
  { to: "/observatories", label: "Observatories" },
  { to: "/camera", label: "Sky Camera" },
];

export default function Navbar() {
  return (
    <header className="border-b border-slate-800 bg-slate-950/80 backdrop-blur">
      <nav className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
        <NavLink to="/" className="text-lg font-semibold tracking-tight">
          Observatory <span className="text-brand-500">Services</span>
        </NavLink>
        <ul className="flex gap-6 text-sm text-slate-300">
          {links.map((l) => (
            <li key={l.to}>
              <NavLink
                to={l.to}
                className={({ isActive }) =>
                  isActive ? "text-white" : "hover:text-white"
                }
              >
                {l.label}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>
    </header>
  );
}
