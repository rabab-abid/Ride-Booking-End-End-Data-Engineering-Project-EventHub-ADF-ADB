CREATE OR REFRESH STREAMING TABLE silver_obt
AS 




    SELECT 
        
            bulk_stream_rides.ride_id, bulk_stream_rides.confirmation_number, bulk_stream_rides.passenger_id, bulk_stream_rides.driver_id, bulk_stream_rides.vehicle_id, bulk_stream_rides.pickup_location_id, bulk_stream_rides.dropoff_location_id, bulk_stream_rides.vehicle_type_id, bulk_stream_rides.vehicle_make_id, bulk_stream_rides.payment_method_id, bulk_stream_rides.ride_status_id, bulk_stream_rides.pickup_city_id, bulk_stream_rides.dropoff_city_id, bulk_stream_rides.cancellation_reason_id, bulk_stream_rides.passenger_name, bulk_stream_rides.passenger_email, bulk_stream_rides.passenger_phone, bulk_stream_rides.driver_name, bulk_stream_rides.driver_rating, bulk_stream_rides.driver_phone, bulk_stream_rides.driver_license, bulk_stream_rides.vehicle_model, bulk_stream_rides.vehicle_color, bulk_stream_rides.license_plate, bulk_stream_rides.pickup_address, bulk_stream_rides.pickup_latitude, bulk_stream_rides.pickup_longitude, bulk_stream_rides.dropoff_address, bulk_stream_rides.dropoff_latitude, bulk_stream_rides.dropoff_longitude, bulk_stream_rides.distance_miles, bulk_stream_rides.duration_minutes, bulk_stream_rides.booking_timestamp, bulk_stream_rides.pickup_timestamp, bulk_stream_rides.dropoff_timestamp, bulk_stream_rides.base_fare, bulk_stream_rides.distance_fare, bulk_stream_rides.time_fare, bulk_stream_rides.surge_multiplier, bulk_stream_rides.subtotal, bulk_stream_rides.tip_amount, bulk_stream_rides.total_fare, bulk_stream_rides.rating 
                
                    ,
                
        
            map_vehicle_makes.vehicle_make 
                
                    ,
                
        
            map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute 
                
                    ,
                
        
            map_ride_statuses.ride_status 
                
                    ,
                
        
            map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth 
                
                    ,
                
        
            map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at 
                
                    ,
                
        
            map_cancellation_reasons.cancellation_reason 
                
        
    FROM 
        
            
                STREAM(bulk_stream_rides)
            
        
            
                LEFT JOIN uber.bronze.map_vehicle_makes map_vehicle_makes ON bulk_stream_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
            
        
            
                LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types ON bulk_stream_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
            
        
            
                LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses ON bulk_stream_rides.ride_status_id = map_ride_statuses.ride_status_id
            
        
            
                LEFT JOIN uber.bronze.map_payment_methods map_payment_methods ON bulk_stream_rides.payment_method_id = map_payment_methods.payment_method_id
            
        
            
                LEFT JOIN uber.bronze.map_cities map_cities ON bulk_stream_rides.pickup_city_id = map_cities.city_id
            
        
            
                LEFT JOIN uber.bronze.map_cancellation_reasons map_cancellation_reasons ON bulk_stream_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
            
        
   
    WHERE 1=1