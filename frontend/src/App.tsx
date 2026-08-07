import { Routes, Route } from "react-router-dom";
import Navbar from "@/components/Navbar";
import Footer from "@/components/Footer";
import Home from "@/routes/Home";
import Hardware from "@/routes/Hardware";
import Software from "@/routes/Software";
import Imaging from "@/routes/Imaging";
import Observatories from "@/routes/Observatories";
import SkycamLayout from "@/routes/skycam/SkycamLayout";
import LiveView from "@/routes/skycam/LiveView";
import Images from "@/routes/skycam/Images";
import Telemetry from "@/routes/skycam/Telemetry";
import NotFound from "@/routes/NotFound";

export default function App() {
  return (
    <div className="flex min-h-full flex-col">
      <Navbar />
      <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-12">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/hardware" element={<Hardware />} />
          <Route path="/software" element={<Software />} />
          <Route path="/imaging" element={<Imaging />} />
          <Route path="/observatories" element={<Observatories />} />
          <Route path="/camera" element={<SkycamLayout />}>
            <Route index element={<LiveView />} />
            <Route path="images" element={<Images />} />
            <Route path="telemetry" element={<Telemetry />} />
          </Route>
          <Route path="*" element={<NotFound />} />
        </Routes>
      </main>
      <Footer />
    </div>
  );
}
