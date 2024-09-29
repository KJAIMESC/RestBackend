require "kafka"

module Api
  module V1
    class EventsController < ApplicationController
      # Send petition to Kafka and wait for response
      def send_kafka_petition(action, data = {})
        kafka = Kafka.new([ "localhost:9092" ], client_id: "events-api")

        # Add a resource identifier to the petition
        petition = {
          resource: "event",  # Identifier for event petitions
          action: action,
          data: data
        }

        # Produce the petition to the "petition" topic
        producer = kafka.async_producer(delivery_interval: 10)
        producer.produce(petition.to_json, topic: "petition")
        producer.shutdown

        # Listen for a response from the "response" topic
        consumer = kafka.consumer(group_id: "events-api-group")
        consumer.subscribe("response")
        response = nil
        consumer.each_message(max_wait_time: 5) do |message|
          response = JSON.parse(message.value)
          break
        end
        consumer.stop
        response
      end

      # GET /api/v1/events
      def index
        response = send_kafka_petition("index")
        if response
          render json: response, status: :ok
        else
          render json: { message: "No response from backend" }, status: :gateway_timeout
        end
      end

      # GET /api/v1/events/:id
      def show
        event_id = params[:id]
        response = send_kafka_petition("show", { event_id: event_id })
        if response
          render json: response, status: :ok
        else
          render json: { message: "No response from backend" }, status: :gateway_timeout
        end
      end

      # POST /api/v1/events
      def create
        data = event_params.to_h
        response = send_kafka_petition("create", data)
        if response
          render json: response, status: :ok
        else
          render json: { message: "No response from backend" }, status: :gateway_timeout
        end
      end

      # PUT /api/v1/events/:id
      def update
        data = event_params.to_h.merge(id: params[:id])
        response = send_kafka_petition("update", data)
        if response
          render json: response, status: :ok
        else
          render json: { message: "No response from backend" }, status: :gateway_timeout
        end
      end

      # DELETE /api/v1/events/:id
      def destroy
        event_id = params[:id]
        response = send_kafka_petition("destroy", { id: event_id })
        if response
          render json: response, status: :ok
        else
          render json: { message: "No response from backend" }, status: :gateway_timeout
        end
      end

      private

      # Strong parameters for event data
      def event_params
        params.require(:event).permit(:name, :location, :date)
      end
    end
  end
end
