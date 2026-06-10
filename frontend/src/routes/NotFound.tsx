import { Link } from "react-router-dom";

export default function NotFound() {
  return (
    <section className="space-y-4 text-center">
      <h1 className="text-3xl font-bold">404</h1>
      <p className="text-slate-300">That page is not in this sky.</p>
      <Link to="/" className="text-brand-500 hover:underline">
        Back to home
      </Link>
    </section>
  );
}
