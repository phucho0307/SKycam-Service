import SettingsPanel from "./SettingsPanel";

export default function Settings() {
  return (
    <div className="max-w-md space-y-4">
      <SettingsPanel />
      <p className="text-xs text-slate-500">
        Lighting = exposure/gain (applied on the camera); gamma/contrast/brightness reshape the
        preview JPEG. Focus/sharpness is a physical lens adjustment (turn &amp; lock the ring) —
        not a setting here. You can also tune these right on the Live View to compare.
      </p>
    </div>
  );
}
