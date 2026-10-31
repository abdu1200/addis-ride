import { useState } from "react";
import { Button } from "./ui/button";

interface DriverRegistrationFormProps {
  onSubmit: (info: { name: string; phoneNumber: string; carPlate: string }) => void;
}

export function DriverRegistrationForm({ onSubmit }: DriverRegistrationFormProps) {
  const [name, setName] = useState("");
  const [phoneNumber, setPhoneNumber] = useState("");
  const [carPlate, setCarPlate] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !phoneNumber || !carPlate) return;
    onSubmit({ name, phoneNumber, carPlate });
  };

  return (
    <main className="min-h-screen flex items-center justify-center bg-gradient-to-b from-white to-gray-50 px-6">
      <form onSubmit={handleSubmit} className="w-full max-w-[380px] flex flex-col gap-4">
        <h2 className="text-[24px] font-medium text-[#12151C]">Driver details</h2>
        <input
          className="border border-[#D8D5CD] rounded-sm px-4 py-3"
          placeholder="Full name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <input
          className="border border-[#D8D5CD] rounded-sm px-4 py-3"
          placeholder="Phone number"
          value={phoneNumber}
          onChange={(e) => setPhoneNumber(e.target.value)}
        />
        <input
          className="border border-[#D8D5CD] rounded-sm px-4 py-3"
          placeholder="Car plate number"
          value={carPlate}
          onChange={(e) => setCarPlate(e.target.value)}
        />
        <Button type="submit" className="w-full">Start Driving</Button>
      </form>
    </main>
  );
}