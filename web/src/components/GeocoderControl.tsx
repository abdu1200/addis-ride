"use client";

import { useEffect } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet-control-geocoder";
import "leaflet-control-geocoder/dist/Control.Geocoder.css";

interface GeocoderControlProps {
  onSelect: (lat: number, lng: number) => void;
}

interface GeocodeEvent {
  geocode: {
    center: L.LatLng;
  };
}

// Define the geocoder instance to include event listening and map insertion methods
type GeocoderInstance = L.Control & {
  on: (type: string, fn: (e: unknown) => void) => GeocoderInstance;
};

export const GeocoderControl = ({ onSelect }: GeocoderControlProps) => {
  const map = useMap();

  useEffect(() => {
    const ControlWithGeocoder = L.Control as unknown as {
      geocoder: (options: Record<string, unknown>) => GeocoderInstance;
    };

    const geocoder = ControlWithGeocoder.geocoder({
      defaultMarkGeocode: false,
      placeholder: "Search for a destination...",
    })
      .on("markgeocode", (e: unknown) => {
        const { center } = (e as GeocodeEvent).geocode;
        onSelect(center.lat, center.lng);
        map.setView(center, 15);
      })
      .addTo(map);

    return () => {
      map.removeControl(geocoder);
    };
  }, [map, onSelect]);

  return null;
};