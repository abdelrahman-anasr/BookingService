import cors from "cors";
import express from "express";
import { ApolloServer , gql } from "apollo-server-express";
import jwt from "jsonwebtoken";
import { DateTimeResolver , JSONResolver } from "graphql-scalars";
import * as dotenv from 'dotenv'
import * as bcrypt from "bcrypt";
import GraphQLUpload from 'graphql-upload/GraphQLUpload.js';
import graphqlUploadExpress from 'graphql-upload/graphqlUploadExpress.js';
import { checkAuth , fetchRole , fetchId} from "./authorizer.ts";
import {PrismaClient } from "@prisma/client";
import {Kafka , Partitioners , logLevel} from "kafkajs";

dotenv.config();

(async function () {

    const prisma = new PrismaClient();

    let rides = [];
    let subzones = [];

    let ridesToday = [];

    let users = [];

    const kafka = new Kafka({
        clientId: "BookingService",
        brokers: [process.env.KAFKA_URL],
    });

    const producer = kafka.producer({
        createPartitioner: Partitioners.LegacyPartitioner
    });

    // producer.logger().setLogLevel(logLevel.DEBUG);

    const consumer = kafka.consumer({ groupId: "BookingService"});

    

    // consumer.logger().setLogLevel(logLevel.DEBUG);

    const shareNotificationBookingCancelled = async (bookingId : string) => {
        await producer.connect();
        const booking = await prisma.booking.findUnique({
            where: {
                id : Number(bookingId)
            }
        });
        const subride = await prisma.subRides.findUnique({
            where: {
                id : booking.rideId
            }
        });
        await producer.send({
            topic: "notificationRequests",
            messages: [{
                key: bookingId.toString(),
                value: JSON.stringify({request: "Booking Cancelled" , subject : "Booking Cancelled" , message : "A booking has been cancelled" , userId : subride.driverId})
            }]
        });
    }

    const notifyRideCancelled = async (rideId : any) => {
        await producer.connect();
        await producer.send({
            topic: "notificationRequests",
            messages: [{
                key: rideId.toString(),
                value: JSON.stringify({request: "Ride Cancelled" , subject : "Ride Cancelled" , message : "The ride with id: " + rideId + " has been cancelled" , userId : rideId})
            }]
        });
    }

    const shareNotificationRideCompleted = async (rideId : number, userId : number) => {
        await producer.connect();
        await producer.send({
            topic: "notificationRequests",
            messages: [{
                key: rideId.toString(),
                value: JSON.stringify({request: "Ride Completed" , subject : "Ride Completed" , message : "The ride with id: " + rideId + " has been completed" , userId : userId})
            }]
        });
    }

    const shareNotificationBookingReminder = async (userId : number , rideTime : Date) => {
        const stringDate = rideTime.toISOString();
        await producer.connect();
        await producer.send({
            topic: "notificationRequests",
            messages: [{
                key: userId.toString(),
                value: JSON.stringify({request: "Booking Reminder" , subject : "Ride Reminder" , message : "This is a reminder that you have a ride scheduled at: " + stringDate , userId : userId}) 
            }]
        });
     }

    const registerPayment = async (booking : any) => {
        await producer.connect();

        const subsetBooking = {id: booking.id , price: booking.price , studentId : booking.studentId};
        await producer.send({
            topic: "payment-details",
            messages: [{
                key: booking.id.toString(),
                value: JSON.stringify(subsetBooking)
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    }

    const requestRefund = async (bookingId : string) => {
        await producer.connect();
        const json = JSON.stringify({bookingId : bookingId});
        await producer.send({
            topic: "refund-request",
            messages: [{
                key: bookingId.toString(),
                value: json
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    }
    
    const notifySeatReduce= async (rideId : string) => {
        const json = JSON.stringify({rideId : rideId});
        await producer.connect();
        await producer.send({
            topic: "seat-reduce",
            messages: [{
                key: rideId,
                value: json
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    };

    const notifySeatIncrease = async (rideId : string) => {
        const json = JSON.stringify({rideId : rideId});
        await producer.connect();
        await producer.send({
            topic: "seat-increase",
            messages: [{
                key: rideId,
                value: json
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    };

    const paymentRequestNotification = async (booking : any , userId : any) => { 
        await producer.connect();
        await producer.send({
            topic: "notificationRequests",
            messages: [{
                key: booking.id.toString(),
                value: JSON.stringify({request: "Request Payment" , subject : "Request Payment for Booking" , message : "Please pay the ride" , userId : userId})
            }]
        });
    }

    

    await consumer.connect();
    await consumer.subscribe({ topics: ["ride-details" , "subzone-details" , "booking-paid" , "notify-passengers" , "ride-cancelled" , "ride-completed" , "user-gender-details"] , fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log("Message is: " + JSON.stringify(message));
            console.log("Topic is: " + topic);
            if(topic === "ride-details")
            {
                console.log("Received Something!");
                const ride = JSON.parse(message.value.toString());
                const prismaRide = await prisma.subRides.create({
                    data : {
                        id: ride.id,
                        driverId : ride.driverId,
                        basePrice : ride.basePrice,
                        girlsOnly: ride.girlsOnly
                    }
                })
                console.log(ride);
                rides.push(ride);
            }
            else if(topic === "subzone-details")
            {
                console.log("Received Something!");
                const subzone = JSON.parse(message.value.toString());
                const prismaSubzone = await prisma.subSubZones.create({
                    data : {
                        subzoneName: subzone.subzoneName,
                        subZonePrice : subzone.subZonePrice
                    }
                })
                console.log(subzone);
                subzones.push(subzone);
            }
            else if(topic === "booking-paid")
            {
                const booking = JSON.parse(message.value.toString());
                const bookingId = booking;
                const updatedBooking = await prisma.booking.update({
                    where: {
                        id : bookingId
                    },
                    data : {
                        status : "Paid"
                    }
                });
            }
            else if(topic === "notify-passengers")
            {
                const jsonReceived = JSON.parse(message.value.toString());
                const bookings = await prisma.booking.findMany({
                    where: {
                        rideId: Number(jsonReceived.rideId)
                    }
                });
                ridesToday.push(jsonReceived.rideId);
                bookings.forEach(booking => {
                    if(booking.status === "Paid")
                    {
                        
                        shareNotificationBookingReminder(booking.studentId , jsonReceived.rideTime);
                    }
                 });
            }
            else if(topic === "ride-cancelled")
            {
                const rideId = JSON.parse(message.value.toString());
                const bookings = await prisma.booking.findMany({
                    where: {
                        rideId: rideId
                    }
                });
                bookings.forEach(booking => {
                    if(booking.status === "Paid" || booking.status === "Payment in Cash")
                    {
                        notifyRideCancelled(rideId);
                    }
                 });
            }
            else if(topic === "ride-completed")
            {
                const rideId = JSON.parse(message.value.toString());
                ridesToday = ridesToday.filter(ride => ride !== rideId);
                const bookings = await prisma.booking.findMany({
                    where: {
                        rideId: rideId
                    }
                });
                bookings.forEach(async (booking) => {
                    await prisma.booking.update({
                        where: {
                            id : booking.id
                        },
                        data : {
                            status : "Completed"
                        }
                    });
                    if(booking.status === "Paid")
                    {
                        shareNotificationRideCompleted(rideId , booking.studentId);
                    }
                 });
            }
            else if(topic === "user-gender-details")
            {
                const user = JSON.parse(message.value.toString());
                const userId = user.id.toString();
                const gender = user.gender;
                const updatedUser = await prisma.subUser.create({
                    data : {
                        id : Number(userId),
                        gender : gender
                    }
                });
                users.push({id: userId , gender: gender});
            }
        },
    });



    const typeDefs = gql`
        scalar DateTime
        scalar Json

        type Area {
        areaName: String!
        basePrice: Float!
        }

        type Ride {
        id: Int!
        driverId: Int!
        time: DateTime!
        areaName: String!
        fromGiu: Boolean!
        girlsOnly: Boolean!
        basePrice: Float!
        seatsLeft: Int!
        active: Boolean!
        }

        type Booking {
            id: Int!
            studentId: Int!
            rideId: Int!
            status: String!
            price: Float!
        }

        type Request {
            id: Int!
            studentId: Int!
            rideId: Int!
            status: String!
            price: Float!
        }

        type SubUser {
            id: Int!
            gender: String!
        }

        type Query {
            addCookies(token: String!): String
            fetchAllRequests: [Request]
            fetchAllBookings: [Booking]
            fetchBooking(id: Int!): Booking
            fetchRequest(id: Int!): Request
            fetchMyBookings: [Booking]
            fetchMyRequests: [Request]
            fetchRequestsForRide(rideId: Int!): [Request]
            fetchBookingsForRide(rideId: Int!): [Booking]
            rejectRequest(id: Int!): Request
            requestPaymentSend(bookingId : Int! , userId : Int!): String
            cancelBooking(id : Int!): Booking
            cancelRequest(id : Int!): Request
            
        }

        type Mutation {
            createRequest(rideId : Int! , subzoneName : String! , paymentOption: String!): Request
            acceptRequest(id: Int!): Booking
        }

    `;

    const resolvers = {
        DateTime : DateTimeResolver,
        Json : JSONResolver,

        Query: {
            addCookies: async(_parent : any , args : any , {req , res} : any) => {
                res.cookie("Authorization" , args.token , {expires: new Date(Date.now() + 45000000) , httpOnly: true , secure: true});
                return "Added Cookie!";
            },
            fetchAllRequests: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const requests = await prisma.request.findMany();
                    return requests;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchAllBookings: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const bookings = await prisma.booking.findMany();
                    return bookings;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchBooking: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const booking = await prisma.booking.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    return booking;
                }
                else if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const booking = await prisma.booking.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    const ride = rides.find(ride => ride.id === booking.rideId);
                    if(ride === undefined)
                        throw new Error("Ride not found");
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    return booking;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchRequest: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const request = await prisma.request.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    return request;
                }
                else if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const request = await prisma.request.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    const ride = rides.find(ride => ride.id === request.rideId);
                    if(ride === undefined)
                        throw new Error("Ride not found");
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    return request;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchMyBookings: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "student" , "driver"] , fetchRole(req.headers.cookie)))
                {
                    const bookings = await prisma.booking.findMany({
                        where: {
                            studentId: fetchId(req.headers.cookie)
                        }
                    });
                    return bookings;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchMyRequests: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "student" , "driver"] , fetchRole(req.headers.cookie)))
                {
                    const requests = await prisma.request.findMany({
                        where: {
                            studentId: fetchId(req.headers.cookie)
                        }
                    });
                    return requests;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchRequestsForRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const requests = await prisma.request.findMany({
                        where: {
                            rideId: args.rideId
                        }
                    });
                    return requests;
                }
                else if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const requests = await prisma.request.findMany({
                        where: {
                            rideId: args.rideId
                        }
                    });
                    const ride = rides.find(ride => ride.id === requests[0].rideId);
                    if(ride === undefined)
                        throw new Error("Ride not found");
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    return requests;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchBookingsForRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const bookings = await prisma.booking.findMany({
                        where: {
                            rideId: args.rideId
                        }
                    });
                    return bookings;
                }
                else if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const bookings = await prisma.booking.findMany({
                        where: {
                            rideId: args.rideId
                        }
                    });
                    const ride = rides.find(ride => ride.id === bookings[0].rideId);
                    if(ride === undefined)
                        throw new Error("Ride not found");
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    return bookings;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            rejectRequest: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const request = await prisma.request.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    const ride = rides.find(ride => ride.id === request.rideId);
                    if(ride === undefined)
                        throw new Error("Ride not found");
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    const updatedRequest = await prisma.request.update({
                        where: {
                            id : args.id
                        },
                        data : {
                            status : "Rejected"
                        }
                    });
                    return updatedRequest;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            requestPaymentSend: async(_parent : any , args : any , {req , res} : any) => {
                const booking = await prisma.booking.findUnique({
                    where: {
                        id : args.bookingId
                    }
                });
                paymentRequestNotification(booking , args.userId);
                return "OK!";
            },
            cancelBooking: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    const booking = await prisma.booking.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    if(booking === null)
                        throw new Error("Booking not found");

                    if(booking.studentId === fetchId(req.headers.cookie))
                    {
                        if(!ridesToday.includes(booking.rideId))
                        {
                            const booking = await prisma.booking.update({
                                where: {
                                    id : args.id
                                },
                                data : {
                                    status : "Cancelled"
                                }
                            });
    
                            await notifySeatIncrease(booking.rideId.toString());
                            await shareNotificationBookingCancelled(booking.id.toString());
                            await requestRefund(booking.id.toString());
                            return booking;
                        }
                        else
                        {
                            throw new Error("You cannot cancel a booking for a ride scheduled for today");
                        }
                    }
                    else
                    {
                        throw new Error("You are not the student of this booking");
                    }
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            cancelRequest: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "student" , "driver"] , fetchRole(req.headers.cookie)))
                {
                    const request = await prisma.request.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    if(request === null)
                        throw new Error("Request not found");

                    if(request.studentId === fetchId(req.headers.cookie))
                    {
                        const request = await prisma.request.update({
                            where: {
                                id : args.id
                            },
                            data : {
                                status : "Cancelled"
                            }
                        });
                        return request;
                    }
                    else
                    {
                        throw new Error("You are not the student of this request");
                    }
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            }
        },
        Mutation: {
            createRequest: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["student" , "driver"] , fetchRole(req.headers.cookie)))
                {
                    const userId = fetchId(req.headers.cookie);
                    let ride = rides.find(ride => ride.id === args.rideId);
                    if(ride === null || ride === undefined)
                    {
                        ride = await prisma.subRides.findUnique({
                            where: {
                                id : args.rideId
                            }
                        });

                        if(ride === null)
                            throw new Error("Ride not found");
                    }
                    if(ride.driverId === fetchId(req.headers.cookie))
                        throw new Error("You are the driver of this ride");

                    const subUser = await prisma.subUser.findFirst({
                        where: {
                            id: fetchId(req.headers.cookie)
                        }
                    });

                    if(ride.girlsOnly === true && subUser.gender === "male")
                        throw new Error("You cannot request a ride for a girls only ride");
                    
                    const subzone = await prisma.subSubZones.findUnique({
                        where: {
                            subzoneName : args.subzoneName
                        }
                    });
                    if(subzone === null)
                        throw new Error("Subzone not found");

                    const requestsFromUser = await prisma.request.findMany({
                        where: {
                            studentId : userId,
                            rideId : args.rideId
                        }
                    });
                    if(requestsFromUser.length > 0)
                    {
                        throw new Error("You have already requested this ride");
                    }

                    const price = ride.basePrice + subzone.subZonePrice;
                    if(args.paymentOption === "Visa")
                    {
                        const request = await prisma.request.create({
                            data : {
                                studentId : userId,
                                rideId : args.rideId,
                                status: "Awaiting Driver's Response - Payment in Visa",
                                subZoneName : args.subzoneName,
                                price : price
                            }
                        });
                        return request;
                    }
                    else
                    {
                        const request = await prisma.request.create({
                            data : {
                                studentId : userId,
                                rideId : args.rideId,
                                status: "Awaiting Driver's Response - Payment in Cash",
                                subZoneName : args.subzoneName,
                                price : price
                            }
                        });
                        return request;
                    }
                    
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            acceptRequest: async(_parent : any , args : any , {req , res} : any) => { 
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const request = await prisma.request.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    let ride = rides.find(ride => ride.id === request.rideId);
                    if(ride === null || ride == undefined)
                    {
                        ride = await prisma.subRides.findUnique({
                            where: {
                                id : request.rideId
                            }
                        });

                        if(ride === null)
                            throw new Error("Ride not found");
                    }
                    console.log("Ride is: " + ride.driverId);
                    if(ride.driverId !== fetchId(req.headers.cookie))
                        throw new Error("You are not the driver of this ride");
                    let booking;
                    if(request.status === "Rejected")
                    {
                        throw new Error("Request has already been rejected");
                    }
                    if(request.status.includes("Payment in Cash"))
                    {
                        booking = await prisma.booking.create({
                            data : {
                                studentId : request.studentId,
                                rideId : request.rideId,
                                status: "Payment In Cash",
                                price: request.price
                            }
                        });
                    }
                    else
                    {
                        booking = await prisma.booking.create({
                            data : {
                                studentId : request.studentId,
                                rideId : request.rideId,
                                status: "Awaiting Payment",
                                price: request.price
                            }
                        });
                        registerPayment(booking);
                    }
                    notifySeatReduce(ride.id.toString());

                    const updatedRequest = await prisma.request.update({
                        where: {
                            id : args.id
                        },
                        data : {
                            status : "Accepted"
                        }
                    });

                    return booking;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            }
        }
    };
    
    const app = express() as any;
    var corsOptions = {
        origin : "http://localhost:3000",
        credentials: true
    }
    app.use(cors(corsOptions));

    const server = new ApolloServer({
        typeDefs, 
        resolvers,
        context: async ({req, res}) => ({
            req , res
        }),

    })

    await server.start();
    console.log("Server started");
    await server.applyMiddleware({app , path : "/booking" , cors: false});
    console.log("Middleware Applied!");

    app.listen({port : 4002} , () => {
        console.log("Server is ready at http://localhost:4002" + server.graphqlPath);

    })
})();