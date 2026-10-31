import { Bus, Zap } from "lucide-react";
import { Car, CarFront } from "lucide-react";
import { CarPackageSlug } from "../types";

export const PackagesMeta: Record<CarPackageSlug, {
  name: string,
  icon: React.ReactNode,
  description: string,
}> = {
  [CarPackageSlug.VITZ]: {
    name: "Vitz",
    icon: <Car />,
    description: "Compact and affordable",
  },
  [CarPackageSlug.COROLLA]: {
    name: "Corolla",
    icon: <CarFront />,
    description: "Comfortable for everyday rides",
  },
  [CarPackageSlug.MINIVAN]: {
    name: "Minivan",
    icon: <Bus />,
    description: "Room for the whole group",
  },
  [CarPackageSlug.BYD]: {
    name: "Byd",
    icon: <Zap />,
    description: "Electric and premium",
  },
}