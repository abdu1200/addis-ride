import { Driver } from "../types";
import Image from "next/image";
import { Phone } from "lucide-react";

export const AssignedDriverCard = ({ driver }: { driver?: Driver | null }) => {
  if (!driver) return null;

  return (
    <div className="w-full rounded-xl border bg-white p-4 flex items-center gap-4">
      {driver.profilePicture ? (
        <Image
          className="rounded-full shrink-0"
          src={driver.profilePicture}
          alt={`${driver.name}'s profile picture`}
          width={56}
          height={56}
        />
      ) : (
        <div className="w-14 h-14 rounded-full bg-gray-100 shrink-0" />
      )}

      <div className="flex-1 min-w-0">
        <p className="font-medium text-gray-900 truncate">{driver.name}</p>
        <p className="text-sm text-gray-500 mt-0.5">
          Plate no. <span className="font-mono tracking-wide text-gray-700">{driver.carPlate?.toUpperCase()}</span>
        </p>
      </div>

      {driver.phoneNumber && (
        <a
          href={`tel:${driver.phoneNumber}`}
          className="shrink-0 flex items-center gap-1.5 rounded-full bg-gray-900 pl-3 pr-4 py-2"
          aria-label={`Call ${driver.name}`}
        >
          <Phone className="w-3.5 h-3.5 text-white" />
          <span className="text-sm text-white">Call</span>
        </a>
      )}
    </div>
  );
};