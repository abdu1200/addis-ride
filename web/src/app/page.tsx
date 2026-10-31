"use client"

// Assets
import 'leaflet/dist/leaflet.css';
// Fix for default marker icon
import icon from 'leaflet/dist/images/marker-icon.png'
import iconShadow from 'leaflet/dist/images/marker-shadow.png'
import dynamic from 'next/dynamic'
import { Button } from "../components/ui/button";
import { useState, Suspense } from "react";
import { useSearchParams, useRouter } from 'next/navigation';
import { CarPackageSlug } from '../types';
import { DriverPackageSelector } from '../components/DriverPackageSelector';
import { DriverRegistrationForm } from '../components/DriverRegistrationForm';

// Dynamically import components that use Leaflet
const DriverMap = dynamic(() => import("../components/DriverMap").then(mod => mod.DriverMap), { ssr: false })
const RiderMap = dynamic(() => import("../components/RiderMap"), { ssr: false })

// Initialize Leaflet icon only on client side
if (typeof window !== 'undefined') {
  import('leaflet').then((L) => {
    const DefaultIcon = L.default.icon({
      iconUrl: icon.src,
      shadowUrl: iconShadow.src,
      iconSize: [25, 41],
      iconAnchor: [12, 41],
    })
    L.default.Marker.prototype.options.icon = DefaultIcon
  })
}

// Stops along the route line on the welcome screen — real Addis Ababa landmarks
const ROUTE_STOPS = [
  { label: 'Piazza', x: 70, y: 90 },
  { label: '6 Kilo', x: 190, y: 190 },
  { label: 'Meskel Sq.', x: 120, y: 320 },
  { label: 'Bole', x: 260, y: 470 },
];

const ROUTE_PATH = 'M70,90 C130,120 150,150 190,190 S150,260 120,320 C160,380 220,410 260,470';

function HomeContent() {
  const [userType, setUserType] = useState<"driver" | "rider" | null>(null)
  const router = useRouter()
  const searchParams = useSearchParams()
  const payment = searchParams.get("payment")
  const [packageSlug, setPackageSlug] = useState<CarPackageSlug | null>(null)
  const [driverInfo, setDriverInfo] = useState<{ name: string; phoneNumber: string; carPlate: string } | null>(null)

  const handleClick = (userType: "driver" | "rider") => {
    setUserType(userType)
  }

  if (payment === 'success') {
    return (
      <main className="min-h-screen bg-gradient-to-b from-white to-gray-50">
        <div className="flex flex-col items-center justify-center h-screen gap-6 px-4">
          <div className="bg-white p-8 rounded-2xl shadow-lg text-center max-w-md w-full">
            <div className="mb-6">
              <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h1 className="text-2xl font-bold text-gray-900">Payment Successful!</h1>
              <p className="text-gray-600 mt-2">Your ride has been confirmed.</p>
            </div>
            <Button
              className="w-full text-lg py-6"
              variant="outline"
              onClick={() => router.push("/")}
            >
              Return Home
            </Button>
          </div>
        </div>
      </main>
    )
  }

  return (
    <main className="min-h-screen bg-gradient-to-b from-white to-gray-50">
      {userType === null && (
        <div className="min-h-screen w-full flex flex-col md:flex-row bg-[#FAF9F6]">
          <style>{`
              @keyframes draw-route {
                  to { stroke-dashoffset: 0; }
              }
              @keyframes pin-in {
                  from { opacity: 0; transform: translateY(4px); }
                  to { opacity: 1; transform: translateY(0); }
              }
              .route-line {
                  stroke-dasharray: 900;
                  stroke-dashoffset: 900;
                  animation: draw-route 2.4s ease-out 0.2s forwards;
              }
              .route-pin {
                  opacity: 0;
                  animation: pin-in 0.5s ease-out forwards;
              }
              @media (prefers-reduced-motion: reduce) {
                  .route-line { animation: none; stroke-dashoffset: 0; }
                  .route-pin { animation: none; opacity: 1; }
              }
          `}</style>

          {/* Left: the map panel */}
          <div className="relative md:w-[46%] min-h-[340px] md:min-h-screen bg-[#12151C] overflow-hidden flex flex-col justify-between gap-8 px-6 py-8 sm:px-8 sm:py-10 md:px-10 md:py-14 lg:px-14 lg:py-16">
            <div className="relative z-10">
              <p className="font-sans text-[13px] tracking-wide text-[#8B90A0]">
                Addis Ride
              </p>
            <h1 className="font-serif text-[26px] sm:text-[32px] md:text-[28px] lg:text-[36px] xl:text-[44px] 2xl:text-[52px] leading-[1.15] lg:leading-[1.1] text-[#FAF9F6] mt-4 md:mt-6 max-w-[320px] sm:max-w-[520px] md:max-w-full lg:max-w-[440px] xl:max-w-[480px] 2xl:max-w-[560px]">
              Wherever you&apos;re headed in Addis, there&apos;s a road to it.
            </h1>
            </div>

            <svg
              viewBox="0 0 360 560"
              className="absolute inset-0 w-full h-full opacity-25 md:opacity-90 md:translate-y-[12%]" 
              preserveAspectRatio="xMidYMid slice"
              aria-hidden="true"
            >
              <path
                d={ROUTE_PATH}
                fill="none"
                stroke="#2A3140"
                strokeWidth="2"
              />
              <path
                d={ROUTE_PATH}
                fill="none"
                stroke="#F2B84B"
                strokeWidth="2.5"
                strokeLinecap="round"
                className="route-line"
              />
              {ROUTE_STOPS.map((stop, i) => (
                <g
                  key={stop.label}
                  className="route-pin"
                  style={{ animationDelay: `${1.4 + i * 0.35}s` }}
                >
                  <circle cx={stop.x} cy={stop.y} r="4.5" fill="#12151C" stroke="#F2B84B" strokeWidth="2" />
                  <text
                    x={stop.x + 12}
                    y={stop.y + 4}
                    className="font-sans"
                    fontSize="13"
                    fill="#C7CAD4"
                  >
                    {stop.label}
                  </text>
                </g>
              ))}
            </svg>

            <p className="relative z-10 font-sans text-[12px] md:text-[13px] text-[#6B7280] max-w-[300px]">
              Live drivers moving across the city right now.
            </p>
          </div>
          {/* Right: the actual choice */}
          <div className="flex-1 flex items-center justify-center px-6 py-12 md:px-16">
            <div className="w-full max-w-[380px]">
              <h2 className="font-serif text-[28px] text-[#12151C]">
                How are you riding today?
              </h2>
              <p className="font-sans text-[15px] text-[#6B7280] mt-2 mb-10">
                Pick one — you can switch anytime from your profile.
              </p>

              <div className="flex flex-col gap-3">
                <button
                  onClick={() => handleClick("rider")}
                  className="group relative text-left w-full px-6 py-5 bg-[#12151C] rounded-sm overflow-hidden
                                        focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#12151C]
                                        transition-colors"
                >
                  <span className="absolute left-0 top-0 h-full w-[3px] bg-[#F2B84B] scale-y-0 group-hover:scale-y-100 group-focus-visible:scale-y-100 origin-top transition-transform duration-300" />
                  <span className="font-sans font-medium text-[16px] text-[#FAF9F6]">
                    I need a ride
                  </span>
                  <span className="block font-sans text-[13px] text-[#9298A6] mt-1">
                    Get picked up and go
                  </span>
                </button>

                <button
                  onClick={() => handleClick("driver")}
                  className="group relative text-left w-full px-6 py-5 bg-transparent border border-[#D8D5CD] rounded-sm overflow-hidden
                                        focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#12151C]
                                        transition-colors hover:border-[#12151C]"
                >
                  <span className="absolute left-0 top-0 h-full w-[3px] bg-[#1F7A4D] scale-y-0 group-hover:scale-y-100 group-focus-visible:scale-y-100 origin-top transition-transform duration-300" />
                  <span className="font-sans font-medium text-[16px] text-[#12151C]">
                    I want to drive
                  </span>
                  <span className="block font-sans text-[13px] text-[#6B7280] mt-1">
                    Set your own hours, earn per trip
                  </span>
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {userType === "driver" && !driverInfo && (
        <DriverRegistrationForm onSubmit={setDriverInfo} />
      )}

      {userType === "driver" && driverInfo && !packageSlug && (
        <DriverPackageSelector onSelect={setPackageSlug} />
      )}

      {userType === "driver" && driverInfo && packageSlug && (
        <DriverMap packageSlug={packageSlug} name={driverInfo.name} phoneNumber={driverInfo.phoneNumber} carPlate={driverInfo.carPlate} />
      )}

      {userType === "rider" && <RiderMap />}
    </main>
  );
}

export default function Home() {
  return (
    <Suspense fallback={
      <main className="min-h-screen bg-gradient-to-b from-white to-gray-50">
        <div className="flex flex-col items-center justify-center h-screen gap-4">
          <div className="bg-white p-8 rounded-2xl shadow-lg text-center max-w-md w-full">
            <div className="animate-pulse flex flex-col items-center">
              <div className="h-8 w-32 bg-gray-200 rounded mb-4"></div>
              <div className="h-4 w-48 bg-gray-100 rounded"></div>
            </div>
          </div>
        </div>
      </main>
    }>
      <HomeContent />
    </Suspense>
  );
}