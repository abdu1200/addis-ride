import { Trip } from "../types"
import { TripOverviewCard } from "./TripOverviewCard"
import { Button } from "./ui/button"
import { TripEvents } from "../contracts"
import { convertMetersToKilometers, convertSecondsToMinutes } from "@/utils/math"

interface DriverTripOverviewProps {
  trip?: Trip | null,
  status?: TripEvents | null,
  onAcceptTrip?: () => void,
  onDeclineTrip?: () => void
}

export const DriverTripOverview = ({ trip, status, onAcceptTrip, onDeclineTrip }: DriverTripOverviewProps) => {
  if (!trip) {
    return (
      <TripOverviewCard
        title="Waiting for a rider..."
        description="Waiting for a rider to request a trip..."
      />
    )
  }

  if (status === TripEvents.DriverTripRequest) {
    return (
      <TripOverviewCard
        title="Trip request received!"
        description="A trip has been requested, check the route and accept the trip if you can take it."
      >
        <div className="flex flex-col gap-4">
          <div className="text-sm text-gray-500">
            <p>Distance: {convertMetersToKilometers(trip.route?.distance ?? 0)}</p>
            <p>Duration: {convertSecondsToMinutes((trip.route?.duration ?? 0) * 2)}</p>
            <p>Fare: {trip.selectedFare?.totalPriceInCents ? ((trip.selectedFare.totalPriceInCents * 2.5) / 100).toFixed(2) : "—"} birr</p>
          </div>

          <div className="flex flex-col gap-2">
            <Button onClick={onAcceptTrip}>Accept trip</Button>
            <Button variant="outline" onClick={onDeclineTrip}>Decline trip</Button>
          </div>
        </div>
      </TripOverviewCard>
    )
  }

  // if (status === TripEvents.DriverTripAccept) {
  //   return (
  //     <TripOverviewCard
  //       title="All set!"
  //       description="You can now start the trip"
  //     >
  //       <div className="flex flex-col gap-4">
  //         <div className="flex flex-col gap-2">
  //           <h3 className="text-lg font-bold">Trip details</h3>
  //           <p className="text-sm text-gray-500">
  //             Trip ID: {trip.id}
  //             <br />
  //             Rider ID: {trip.userID}
  //           </p>
  //         </div>
  //       </div>
  //     </TripOverviewCard>
  //   )
  // }

  if (status === TripEvents.DriverTripAccept) {
    return (
      <TripOverviewCard
        title="All set!"
        description="Please wait for the rider to contact you, then you can start the trip!"
      >
        <div className="grid grid-cols-3 gap-2">
          <div className="rounded-lg bg-gray-50 p-3 text-center">
            <p className="text-xs text-gray-400">Distance</p>
            <p className="text-sm font-medium text-gray-900 mt-1">
              {convertMetersToKilometers(trip.route?.distance ?? 0)}
            </p>
          </div>

          <div className="rounded-lg bg-gray-50 p-3 text-center">
            <p className="text-xs text-gray-400">Duration</p>
            <p className="text-sm font-medium text-gray-900 mt-1">
              {convertSecondsToMinutes((trip.route?.duration ?? 0) * 2)}
            </p>
          </div>

          <div className="rounded-lg bg-gray-50 p-3 text-center">
            <p className="text-xs text-gray-400">Fare</p>
            <p className="text-sm font-medium text-gray-900 mt-1">
              {trip.selectedFare?.totalPriceInCents ? ((trip.selectedFare.totalPriceInCents * 2.5) / 100).toFixed(2) : "—"} birr
            </p>
          </div>
        </div>
      </TripOverviewCard>
    )
  }

  return null
}