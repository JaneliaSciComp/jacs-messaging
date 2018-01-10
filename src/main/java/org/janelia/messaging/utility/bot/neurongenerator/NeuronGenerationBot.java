package org.janelia.messaging.utility.bot.neurongenerator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import com.sun.media.jai.codec.FileSeekableStream;
import com.sun.media.jai.codec.ImageCodec;
import com.sun.media.jai.codec.SeekableStream;
import org.janelia.messaging.broker.sharedworkspace.HeaderConstants;
import org.janelia.messaging.broker.sharedworkspace.MessageType;
import org.janelia.messaging.broker.sharedworkspace.TiledMicroscopeDomainMgr;
import org.janelia.messaging.client.Receiver;
import org.janelia.model.domain.Reference;

import org.apache.commons.cli.*;
import org.janelia.messaging.client.ConnectionManager;
import org.janelia.messaging.client.Sender;
import org.janelia.messaging.utility.bot.neurongenerator.geom.CoordinateAxis;
import org.janelia.messaging.utility.bot.neurongenerator.geom.Rotation3d;
import org.janelia.messaging.utility.bot.neurongenerator.geom.UnitVec3;
import org.janelia.messaging.utility.bot.neurongenerator.geom.Vec3;
import org.janelia.model.domain.tiledMicroscope.*;
import org.janelia.model.util.IdSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.RenderedImageAdapter;

/**
 * Modification of the RandomNeuronGenerator in LVV so that it can feed neuron
 * save messages into the NeuronBroker and generate refresh updates for testing
 *
 * @author <a href="mailto:schauderd@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class NeuronGenerationBot implements DeliverCallback, CancelCallback {

    private static final Logger log = LoggerFactory.getLogger(NeuronGenerationBot.class);

    private static final UnitVec3 UNIT_X = new UnitVec3(CoordinateAxis.X);
    private static final UnitVec3 UNIT_Y = new UnitVec3(CoordinateAxis.Y);
    private static final UnitVec3 UNIT_Z = new UnitVec3(CoordinateAxis.Z);
    private static final double MIN_JUMP_SIZE = 200;
    private static final double MAX_JUMP_SIZE = 300;


    Long workspaceId;
    int numPoints;
    static BoundingBox3d boundingBox;
    double[] scale;
    IdSource idSource;
    String user;
    static Sender neuronMessager;
    static Receiver neuronConfirm;
    static ObjectMapper mapper = new ObjectMapper();
    Map<String,Object> updateHeaders;
    double branchProbability;
    static String messageServer;
    static String messageQueue;
    static String refreshExchange;
    static String routingKey;
    static String persistenceServer;
    static String username;
    static String password;
    static String neuronName;
    static boolean neuronCreated = false;

    // ThreadLocalRandom supports ranges, which Random does not
    private ThreadLocalRandom random = ThreadLocalRandom.current();

    public boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addOption("messageServer", true, "Message Server Host");
        options.addOption("messageQueue", true, "Queue to send requests to.");
        options.addOption("refreshExchange", true, "Exchange to receive updates from");
        options.addOption("dbServer", true, "Database REST server used for persistence");
        options.addOption("routingKey", true, "RoutingKey for the queue");
        options.addOption("workspace", true, "Workspace to generate fake neurons");
        options.addOption("points", false, "Number of anchors in the neuron");
        options.addOption("u", true, "Username for message server");
        options.addOption("p", true, "Password for message server");
        options.addOption("branchProb", false, "Branch Probability");
        options.addOption("neuronOwner", true, "Owner for the Created Neuron");


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            messageServer = cmd.getOptionValue("messageServer");
            messageQueue = cmd.getOptionValue("messageQueue");
            persistenceServer = cmd.getOptionValue("dbServer");
            workspaceId = Long.parseLong(cmd.getOptionValue("workspace"));
            refreshExchange = cmd.getOptionValue("refreshExchange");
            routingKey = cmd.getOptionValue("routingKey");
            user = cmd.getOptionValue("neuronOwner");
            if (cmd.hasOption("branchProb")) {
                branchProbability = Float.parseFloat(cmd.getOptionValue("branchProb"));
            } else {
                branchProbability = 0.2;
            }
            if (cmd.hasOption("numPoints")) {
                numPoints = Integer.parseInt(cmd.getOptionValue("points"));
            } else {
                numPoints = 500;
            }
            username = cmd.getOptionValue("u");
            password = cmd.getOptionValue("p");
        } catch (ParseException e) {
            System.out.println ("Error trying to parse command-line arguments");
            return help(options);
        }
        return true;
    }

    private boolean help (Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("NeuronGenerationBot", options);
        return false;
    }

    public static void main (String args[]) {
        try {
            NeuronGenerationBot bot = new NeuronGenerationBot();

            if (bot.parseArgs(args)) {
                bot.generateArtificialNeuronData();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int[] calculateVolumeSize(TmSample sample) throws Exception {
        // replace this if running on mac
        File topFolderParam = new File(sample.getFilepath().replaceAll("nrs","Volumes"));
        
        int octreeDepth = (int)sample.getNumImageryLevels().longValue();
        int zoomFactor = (int) Math.pow(2, octreeDepth - 1);

        // Deduce other parameters from first image file contents
        File tiff = new File(topFolderParam, "default.0.tif");
        SeekableStream s = new FileSeekableStream(tiff);
        com.sun.media.jai.codec.ImageDecoder decoder = ImageCodec.createImageDecoder("tiff", s, null);
        // Z dimension is related to number of tiff pages
        int sz = decoder.getNumPages();

        // Get X/Y dimensions from first image
        RenderedImageAdapter ria = new RenderedImageAdapter(decoder.decodeAsRenderedImage(0));
        int sx = ria.getWidth();
        int sy = ria.getHeight();
        log.info("sx,sy,sz is {},{},{}",sx,sy,sz);

        // Full volume could be much larger than this downsampled tile
        int[] volumeSize = new int[3];
        volumeSize[2] = zoomFactor * sz;
        volumeSize[0] = zoomFactor * sx;
        volumeSize[1] = zoomFactor * sy;
        return volumeSize;
    }

    private void calcBoundingBox(Long workspaceId) throws Exception {
        // fetch the sample and extract the origin and scale attributes
        TiledMicroscopeDomainMgr domainMgr = new TiledMicroscopeDomainMgr(persistenceServer);
        TmSample sample = domainMgr.getSampleByWorkspaceId(workspaceId, user);

        // if no origin or scale attributes, stop (old sample)
        if (sample==null || sample.getOrigin()==null || sample.getScaling()==null)
            return;

        // calculate volumeSize
        int[] volumeSize = calculateVolumeSize(sample);
        log.info("volume size is + {}", volumeSize);
        // get origin and scale from Sample
        int[] origin = new int[3];
        for (int i=0; i<3; i++) {
            origin[i] = sample.getOrigin().get(i);
        }
        scale = new double[3];
        for (int i=0; i<3; i++) {
            scale[i] = sample.getScaling().get(i);
        }

        double divisor = Math.pow(2.0, sample.getNumImageryLevels().intValue() - 1);
        for (int i = 0; i < scale.length; i++) {
            scale[ i] /= divisor; // nanometers to micrometers
        }

        for (int i = 0; i < scale.length; i++) {
            scale[ i] /= 1000; // nanometers to micrometers
        }
        for (int i = 0; i < origin.length; i++) {
            origin[ i] = (int) (origin[i] / (1000 * scale[i])); // nanometers to voxels
        }
        log.info("origin is + {}", origin);
        log.info("scale is + {}", scale);

        // calculate bounding box using scale, origin, and volume
        Vec3 b0 = new Vec3(0,0,0);
        Vec3 b1 = new Vec3(volumeSize[0], volumeSize[1], volumeSize[2]);
        boundingBox = new BoundingBox3d();
        boundingBox.setMin(b0);
        boundingBox.setMax(b1);

        log.info("Bounding box is {} - {}", b0, b1);
    }

    public void generateArtificialNeuronData() throws Exception {
        idSource = new IdSource((int) (numPoints * 20));

        // create neuron for this workspace
        ConnectionManager connManager = ConnectionManager.getInstance();
        connManager.configureTarget(messageServer, username, password);
        neuronMessager = new Sender();
        neuronMessager.init(connManager, messageQueue, routingKey);

        Reference workspaceRef = Reference.createFor("org.janelia.model.domain.tiledMicroscope.TmWorkspace",
                workspaceId);
        TmNeuronMetadata neuron = new TmNeuronMetadata();
        TmProtobufExchanger exchanger = new TmProtobufExchanger();
        neuron.setWorkspaceRef(workspaceRef);
        neuronName = "Bot Generated Neuron #" + new Random().nextInt(1000000);
        neuron.setName(neuronName);
        exchanger.deserializeNeuron(null, neuron);
        neuron.setOwnerKey(user);

        updateHeaders = new HashMap<String, Object>();
        updateHeaders.put(HeaderConstants.TYPE, MessageType.NEURON_CREATE.toString());
        updateHeaders.put(HeaderConstants.USER, user);
        updateHeaders.put(HeaderConstants.WORKSPACE, workspaceRef.getTargetId().toString());
        updateHeaders.put(HeaderConstants.METADATA, mapper.writeValueAsString(neuron));

        byte[] msgBody = null;
        neuronMessager.sendMessage(updateHeaders, msgBody);

        // listen for creation of the neuron with an id
        neuronConfirm = new Receiver();
        neuronConfirm.init(connManager, refreshExchange, true);
        neuronConfirm.setupReceiver(this);
    }

    private String convertLongString (LongString data) {
        return LongStringHelper.asLongString(data.getBytes()).toString();
    }

    @Override
    public void handle(String consumerTag, Delivery message) {
        if (neuronCreated)
            return;
        Map<String, Object> msgHeaders = message.getProperties().getHeaders();
        try {
            if (msgHeaders != null) {
                Long workspace = Long.parseLong(convertLongString((LongString) msgHeaders.get(HeaderConstants.WORKSPACE)));
                MessageType action = MessageType.valueOf(convertLongString((LongString) msgHeaders.get(HeaderConstants.TYPE)));
                String metadata = convertLongString((LongString) msgHeaders.get(HeaderConstants.METADATA));

                if (metadata != null && action == MessageType.NEURON_CREATE) {
                    ObjectMapper mapper = new ObjectMapper();
                    TmNeuronMetadata neuron = mapper.readValue(metadata, TmNeuronMetadata.class);
                    if (!neuron.getName().equals(neuronName))
                        return;
                    TmProtobufExchanger exchanger = new TmProtobufExchanger();
                    exchanger.deserializeNeuron(new ByteArrayInputStream(message.getBody()),neuron);

                    neuronCreated = true;
                    // we have the right neuron and it's been created properly
                    calcBoundingBox(neuron.getWorkspaceId());
                    startNeuronGeneration(neuron);
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startNeuronGeneration(TmNeuronMetadata neuron) throws Exception {
        Long neuronId = neuron.getId();
        log.info("Generating random neuron {} with {} points", neuron.getName(), numPoints);

        // Choose a starting point for the neuron
        Vec3 startingPoint = getRandomPoint();

        log.info("Random starting point: {}", startingPoint);

        // Current direction of every branch
        Map<TmGeoAnnotation, Vec3> branchDirections = new HashMap<>();

        // Current set of neuron end points which can be extended
        List<TmGeoAnnotation> endPoints = new LinkedList<>();

        // Create root annotation at the starting point
        TmGeoAnnotation rootAnnotation = new TmGeoAnnotation();
        rootAnnotation.setId(nextGuid());
        rootAnnotation.setNeuronId(neuronId);
        rootAnnotation.setParentId(neuronId);
        rootAnnotation.setCreationDate(new Date());
        rootAnnotation.setX(startingPoint.getX());
        rootAnnotation.setY(startingPoint.getY());
        rootAnnotation.setZ(startingPoint.getZ());
        log.info("Creating root annotation = {}",rootAnnotation);

        // Choose a direction and start tracing in that direction
        branchDirections.put(rootAnnotation, getRandomUnitVector());
        endPoints.add(rootAnnotation);
        neuron.addRootAnnotation(rootAnnotation);

        Map<Long, TmGeoAnnotation> map = neuron.getGeoAnnotationMap();
        map.put(rootAnnotation.getId(), rootAnnotation);


        // set up message header ahead of time, so we can just change the neuron data
        // we have a new version of the neuron now; send an update
        Map<String,Object> msgHeaders = new HashMap<String,Object>();
        msgHeaders.put(HeaderConstants.TYPE, MessageType.NEURON_SAVE_NEURONDATA.toString());
        List<String> neuronIds = new ArrayList<>();
        neuronIds.add(neuron.getId().toString());
        msgHeaders.put(HeaderConstants.NEURONIDS, neuronIds);
        msgHeaders.put(HeaderConstants.USER, user);
        msgHeaders.put(HeaderConstants.WORKSPACE, neuron.getWorkspaceId().toString());

        int i = 1;
        while(i < numPoints)  {
            // add a delay of 1 second in between sending out messages
            Thread.sleep(1000);

            // Choose an end point to extend
            int branchIndex = (int) (endPoints.size() * Math.random());
            TmGeoAnnotation parent = endPoints.get(branchIndex);

            Float rot = null;
            if (random.nextDouble()<branchProbability && i>10) {
                // Branch at the grandparent so that each branch has at least one segment
                TmGeoAnnotation grandparent = map.get(map.get(parent.getId()).getParentId());
                if (grandparent!=null) {
                    // Start at grandparent and branch
                    parent = grandparent;
                    // Rotate more when branching
                    rot = 0.20f;
                }
                else {
                    // No grandparent could be found. This is probably because we went out
                    // of bounds and restarted at the root. That's okay, it just means
                    // we can't branch here. Logic will fall through to the normal end point
                    // processing.
                }
            }

            if (rot==null) {
                // Rotate very slightly
                rot = 0.05f;
                // Parent is no longer an end point
                endPoints.remove(parent);
            }

            // End point we're extending
            Vec3 lastPoint = new Vec3(parent.getX(), parent.getY(), parent.getZ());
            Vec3 direction = branchDirections.get(parent);

            // Calculate next direction by slightly perturbing the last direction
            direction = rotateSlightly(direction, rot);

            // Calculate next point location
            double jumpSize = getRandomBoundedDouble(MIN_JUMP_SIZE, MAX_JUMP_SIZE);
            Vec3 point = lastPoint.plus(direction.times(jumpSize));

            if (!boundingBox.contains(point)) {
                log.info("Out of bounds, starting over: {}",point);
                // Out of bounds, stop extending this branch
                endPoints.remove(parent);
                // If there are no other possible end points, start again at the root, in a new direction
                if (endPoints.isEmpty()) {
                    endPoints.add(rootAnnotation);
                    branchDirections.put(rootAnnotation, getRandomUnitVector());
                }
                continue;
            }

            // Create annotation
            TmGeoAnnotation geoAnnotation = new TmGeoAnnotation();
            geoAnnotation.setId(nextGuid());
            geoAnnotation.setNeuronId(neuronId);
            geoAnnotation.setParentId(parent.getId());
            geoAnnotation.setX(point.getX());
            geoAnnotation.setY(point.getY());
            geoAnnotation.setZ(point.getZ());
            geoAnnotation.setCreationDate(new Date());

            branchDirections.put(geoAnnotation, direction);
            endPoints.add(geoAnnotation);
            map.put(geoAnnotation.getId(), geoAnnotation);
            parent.addChild(geoAnnotation);

            i++;

            log.info("New annotation created = {}",geoAnnotation);
            msgHeaders.put(HeaderConstants.METADATA, mapper.writeValueAsString(neuron));
            TmProtobufExchanger exchanger = new TmProtobufExchanger();
            byte[] msgBody = exchanger.serializeNeuron(neuron);
            neuronMessager.sendMessage(msgHeaders, msgBody);
        }
    }

    private Vec3 getRandomPoint() {
        Random rand = new Random();
        return new Vec3(
                boundingBox.getMinX() + boundingBox.getWidth()*(0.2 + 0.6*rand.nextFloat()),
                boundingBox.getMinY() + boundingBox.getHeight()*(0.2 + 0.6*rand.nextFloat()),
                boundingBox.getMinZ() +  boundingBox.getDepth()*(0.2 + 0.6*rand.nextFloat())
        );
    }

    private Vec3 getRandomUnitVector() {
        Vec3 v = new Vec3(
                random.nextGaussian(),
                random.nextGaussian(),
                random.nextGaussian()
        );
        // Normalize to unit vector
        double mag = getMagnitude(v);
        v.multEquals(1/mag);
        return v;
    }

    private double getMagnitude(Vec3 v) {
        return Math.sqrt(Math.pow(v.x(), 2) + Math.pow(v.y(), 2) + Math.pow(v.z(), 2));
    }

    private double getRandomBoundedDouble(double min, double max) {
        double r = min + random.nextGaussian()*((max - min)/2);
        // If it's out of range, use a uniform distribution instead
        if (r<min || r>max) return random.nextDouble(min, max + 1);
        return r;
    }

    private Vec3 rotateSlightly(Vec3 v, double radMax) {
        v = rotateSlightly(UNIT_X, v, radMax);
        v = rotateSlightly(UNIT_Y, v, radMax);
        v = rotateSlightly(UNIT_Z, v, radMax);
        return v;
    }

    private Vec3 rotateSlightly(UnitVec3 rotationAxis, Vec3 v, double radMax) {
        double rotationAngle = radMax * Math.PI * random.nextGaussian(); // Very small angle adjustments
        Rotation3d rotation = new Rotation3d().setFromAngleAboutUnitVector(rotationAngle, rotationAxis);
        return rotation.times(v);
    }

    private long nextGuid() {
        return idSource.next();
    }

    @Override
    public void handle(String consumerTag) throws IOException {
        throw new UnsupportedOperationException("This method is not supported yet");
    }
}
