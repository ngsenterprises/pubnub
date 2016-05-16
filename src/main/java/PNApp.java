import java.util.Hashtable;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.json.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.pubnub.api.*;


class MyDataCallback extends Callback {
    public static final int dataSize = 500;
    public Pubnub context = null;

    List<Integer> data = new ArrayList<Integer>();

    List<Integer> lower = new ArrayList<Integer>();
    List<Integer> pivoted = new ArrayList<Integer>();
    List<Integer> upper = new ArrayList<Integer>();

    public void printNums(List<Integer> pnums) {
        //System.out.println();
        System.out.print("(");
        if (0 < pnums.size())
            System.out.print(pnums.get(0));
        for (int i = 1; i < pnums.size(); i++)
            System.out.print(", " +pnums.get(i));
        System.out.println(")");
    }

    public void doPivot(List<Integer> d, Integer pivot) {
        lower.clear();
        pivoted.clear();
        upper.clear();
        for (int k: d) {
            if (k < pivot) lower.add(k);
            else if (k == pivot) pivoted.add(k);
            else upper.add(k);
        }

        System.out.println();
        System.out.println("Worker doPivot:  " +pivot);
        System.out.print("lower: ");
        printNums(lower);
        System.out.print("pivoted: ");
        printNums(pivoted);
        System.out.print("upper: ");
        printNums(upper);

//        for (int i = 0; i < lower.size(); i++) System.out.print(lower.get(i) +",");
//        System.out.println(")");
//        System.out.print("pivoted: (");
//        for (int i = 0; i < pivoted.size(); i++) System.out.print(pivoted.get(i) +",");
//        System.out.println(")");
//        System.out.print("upper: (");
//        for (int i = 0; i < upper.size(); i++) System.out.print(upper.get(i) +",");
//        System.out.println(")");

    }

    public List<Integer> getSegment(String seg) {
        if (seg.compareToIgnoreCase("lower") == 0)
            return lower;
        if (seg.compareToIgnoreCase("pivoted") == 0)
            return pivoted;
        if (seg.compareToIgnoreCase("upper") == 0)
            return upper;
        return null;
    }


}//end class MyDataCallback



class Worker extends MyDataCallback {

    String supervisor;

    public Worker(String supervisor, Pubnub cxt) {
        //System.out.println();
        Random rnd = new Random();
        for (int i = 0; i < dataSize; i++) {
            int ri = rnd.nextInt(100);
            data.add(ri);
            lower.add(ri);
        }
        context = cxt;
        this.supervisor = supervisor;
        //printNums(lower);
    }

    public void connectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE: WorkerCallBack.connectCallback " +channel +" : " + message.getClass() + " : " +message.toString());
        //pb.publish(channel, "connectCallback", new Callback() {});
    }
    @Override
    public void disconnectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE: WorkerCallBack.disconnectCallback " + channel +" : " + message.getClass() + " : " +message.toString());
    }
    public void reconnectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE : WorkerCallBack.reconnectCallback: " + channel +" : " +message.getClass() + " : " +message.toString());
    }
    @Override
    public void successCallback(String channel, Object message) {
        System.out.println();
        System.out.println("SUBSCRIBE WorkerCallBack.successCallback : " + channel + " : " +message.getClass() + " : " + message.toString());

        try {
            JSONObject jsonObject = (JSONObject) message;
            int iteration = (Integer)jsonObject.get("iteration");
            System.out.println("iteration = " +iteration);

            if (iteration == 0) {
                JSONObject jsonObj = null;
                try {
                    int sz = lower.size();
                    jsonObj = new JSONObject();
                    jsonObj.put("iteration", iteration);
                    jsonObj.put("datasize", sz);

                } catch (JSONException e) {
                    e.printStackTrace();
                }

                if (jsonObj != null) {
                    context.publish(supervisor, jsonObj, new Callback() {});
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                }

            }
            else {
                String segment = (String) jsonObject.get("segment");
                System.out.println("segment: " + segment);
                int pivot = (Integer) jsonObject.get("pivot");
                System.out.println("pivot: " + pivot);

                List<Integer> ta = new ArrayList<Integer>();
                if (segment.compareToIgnoreCase("lower") == 0) for (int k : lower) ta.add(k);
                else if (segment.compareToIgnoreCase("upper") == 0) for (int k : upper) ta.add(k);

                doPivot(ta, pivot);

                JSONObject jsonObj = null;
                try {
                    int lowersize = lower.size();
                    int uppersize = upper.size();
                    //System.out.println("L: " + lowersize +" P: " +pivoted.size() +" U: " +uppersize);
                    jsonObj = new JSONObject();
                    jsonObj.put("iteration", iteration);
                    jsonObj.put("lowersize", lowersize);
                    jsonObj.put("pivotedsize", pivoted.size());
                    jsonObj.put("uppersize", uppersize);
                    //jsonObj.put("pivotused", pivot);
                    if (0 < lower.size())
                        jsonObj.put("lowerpivot", lower.get(lowersize -1));
                    else
                        jsonObj.put("lowerpivot", -1);
                    if (0 < upper.size())
                        jsonObj.put("upperpivot", upper.get(uppersize -1));
                    else
                        jsonObj.put("upperpivot", -1);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                if (jsonObj != null) {
                    context.publish(supervisor, jsonObj, new Callback() {});
                    try { Thread.sleep(1000); }
                    catch (InterruptedException e) {}
                }
            }

        } catch(JSONException je) {
            System.out.println("WorkerCallBack.successCallback " + je.toString());
        }
    }
    @Override
    public void errorCallback(String channel, PubnubError error) {
        System.out.println("SUBSCRIBE : WorkerCallBack.errorCallback " + channel +" : " +error.toString());
    }
}//end class Worker


class Supervisor extends MyDataCallback {

    String[] workers;
    private int currentIteration = -1;
    private int nextIteration = -1;
    List<JSONObject> itrBldr = new ArrayList<JSONObject>();
    //int aggDataSize = 0;
    List<Integer> medianOffsets = new ArrayList<Integer>();
    List<Integer> medianValues = new ArrayList<Integer>();
    //int pivotLB = Integer.MIN_VALUE;
    //int pivotUB = Integer.MAX_VALUE;
    int pivotLast;






    public Supervisor(String[] ws, Pubnub cxt) {
        Random rnd = new Random();
        for (int i = 0; i < dataSize; i++) {
            int ri = rnd.nextInt(100);
            data.add(ri);
            lower.add(ri);
        }
        workers = ws.clone();
        context = cxt;
    }

    public void abEnd() {
        context.shutdown();
    }

    public double terminate() {
        double sum = 0.0;
        for (int r: medianValues)
            sum += (double)r;
        double median = sum/(double)medianValues.size();
        System.out.println("median: " +median);

        context.shutdown();
        return median;
    }


    public int getNextIteration() {
        nextIteration += 1;
        currentIteration = nextIteration;
        return nextIteration;
    }


    public void doIteration(Pubnub pb, String segment, int pivot) {
        List<Integer> ta = new ArrayList<Integer>();
        List<Integer> dat = getSegment(segment);
        pivotLast = pivot;
        for (int i : dat)
            ta.add(i);

        doPivot(ta, pivot);

        JSONObject jsonObj = null;
        try {
            jsonObj = new JSONObject();
            jsonObj.put("iteration", getNextIteration());
            jsonObj.put("pivot", pivot);
            jsonObj.put("segment", segment);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        if (jsonObj != null) {
            for (String w : workers)
                pb.publish(w, jsonObj, new Callback() {});
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }



    public void start(Pubnub pb) {

        JSONObject jsonObj = null;
        try {
            jsonObj = new JSONObject();
            jsonObj.put("iteration", getNextIteration());
        } catch (JSONException e) {
            e.printStackTrace();
        }

        if (jsonObj != null) {
            for (String w : workers) pb.publish(w, jsonObj, new Callback() {});
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }
    }

    public void connectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE: SupervisorCallBack.connectCallback " + channel + " : " + message.getClass() + " : " + message.toString());
        //pb.publish(channel, "connectCallback", new Callback() {});
    }
    @Override
    public void disconnectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE: SupervisorCallBack.disconnectCallback " + channel + " : " + message.getClass() + " : " + message.toString());
    }
    public void reconnectCallback(String channel, Object message) {
        System.out.println("SUBSCRIBE : SupervisorCallBack.reconnectCallback: " + channel +" : " +message.getClass() + " : " +message.toString());
    }
    @Override
    public void successCallback(String channel, Object message) {

        System.out.println();
        System.out.println("SUBSCRIBE SupervisorCallBack.successCallback : " + channel + " : " +message.getClass() + " : " + message.toString());

        try {
            JSONObject jsonObject = (JSONObject) message;
            int iteration = (Integer) jsonObject.get("iteration");

            //INITAL ITERATION
            if (iteration == 0) {
                if (itrBldr.size() < workers.length) {
                    itrBldr.add(jsonObject);
                    if (itrBldr.size() == workers.length) {
                        int aggDataSize = lower.size();
                        for (JSONObject jo : itrBldr)
                            aggDataSize += (Integer) jo.get("datasize");
                        System.out.println("aggDataSize: " + aggDataSize);

                        int m0 = aggDataSize / 2 - 1;
                        if (aggDataSize % 2 == 0)
                            medianOffsets.add(0, m0 + 1);
                        medianOffsets.add(0, m0);

                        itrBldr.clear();

                        doIteration(context, "lower", lower.get(0));
                    }
                }
            }
            else if (iteration == currentIteration) {
                if (itrBldr.size() < workers.length) {
                    itrBldr.add(jsonObject);
                    if (itrBldr.size() == workers.length) {

                        List<Integer> lpivots = new ArrayList<Integer>();
                        List<Integer> upivots = new ArrayList<Integer>();

                        int aggLowerSize = lower.size();
                        int aggPivotedSize = pivoted.size();
                        for (JSONObject jo: itrBldr) {
                            aggLowerSize += jo.getInt("lowersize");
                            aggPivotedSize += jo.getInt("pivotedsize");
                            int p = jo.getInt("lowerpivot");
                            if (0 <= p)
                                lpivots.add(p);
                            p = jo.getInt("upperpivot");
                            if (0 <= p)
                                upivots.add(p);
                        }
                        if (0 < lower.size())
                            lpivots.add(lower.get(0));
                        if (0 < upper.size())
                            upivots.add(upper.get(0));

                        System.out.println();
                        System.out.println("CONDENSE ITERATION: " +iteration);
                        System.out.println("aggLowerSize: " +aggLowerSize);
                        System.out.println("aggPivotedSize: " +aggPivotedSize);
                        System.out.println("lower pivot cache size: " + lpivots.size());
                        System.out.println("upper pivot cache size: " + upivots.size());

                        int lastMedianIndex = medianOffsets.size() -1;

                        //LOWER SEGMENT COMPLETE
                        if (medianOffsets.get(lastMedianIndex) < aggLowerSize) {
                            if (!lpivots.isEmpty()) {
                                doIteration(context, "lower", lpivots.get(0));
                            }
                            else {
                                System.out.println("ERROR: No lower pivot.");
                                abEnd();
                            }
                        }

                        //LOWER SEGMENT PARTIALY PIVOTED
                        else if (medianOffsets.get(lastMedianIndex) == aggLowerSize) {
                            medianValues.add(pivotLast);
                            medianOffsets.remove(1);

                            if (!medianOffsets.isEmpty()) {
                                if (!lpivots.isEmpty()) {
                                    doIteration(context, "lower", lpivots.get(0));
                                }
                                else {
                                    System.out.print("ERROR: No lower pivot.");
                                    abEnd();
                                }
                            }
                            else
                                terminate();
                        }

                        //PIVOTED SEGMENT
                        else if (medianOffsets.get(lastMedianIndex) < aggLowerSize +aggPivotedSize) {

                            medianValues.add(pivotLast);
                            medianOffsets.remove(1);

                            if (!medianOffsets.isEmpty()) {
                                medianValues.add(pivotLast);
                                medianOffsets.remove(0);
                            }
                            terminate();
                        }

                        //UPPER SEGMENT PARTIALLY PIVOTED
                        else if (medianOffsets.get(0) == aggLowerSize +aggPivotedSize -1) {
                            medianValues.add(pivotLast);
                            medianOffsets.remove(0);

                            if (!medianOffsets.isEmpty()) {
                                int v = medianOffsets.get(0);
                                medianOffsets.set(0, v -aggLowerSize -aggPivotedSize);

                                if (!upivots.isEmpty()) {
                                    doIteration(context, "upper", upivots.get(0));
                                }
                                else {
                                    System.out.println("ERROR: No upper pivot.");
                                    abEnd();
                                }
                            }
                            else
                                terminate();
                        }

                        //UPPER SEGMENT COMPLETE
                        else {
                            for (int i = 0; i < medianOffsets.size(); i++) {
                                int v = medianOffsets.get(i);
                                medianOffsets.set(i, v -aggLowerSize -aggPivotedSize);
                            }

                            if (!upivots.isEmpty()) {
                                doIteration(context, "upper", upivots.get(0));
                            }
                            else {
                                System.out.println("ERROR: No upper pivot.");
                                abEnd();
                            }
                        }
                        itrBldr.clear();
                    }
                }
            }
        } catch(JSONException je) {
            je.printStackTrace();
        }




    }
    @Override
    public void errorCallback(String channel, PubnubError error) {
        System.out.println("SUBSCRIBE : SupervisorCallBack.errorCallback " + channel +" : " +error.toString());
    }
}//end class Supervisor


public class PNApp {

    public static String worker_1_channel = "worker_1_channel";
    public static String worker_2_channel = "worker_2_channel";
    public static String worker_3_channel = "worker_3_channel";
    public static String supervisor_channel = "supervisor_channel";
    public static final int numChannels = 4;
    public static final int dataSize = 500;

    public static void main(String[] args) {

        final Pubnub pubnub = new Pubnub("pub-c-c8ca56fc-9b4b-4120-affc-314e4c1620f4", "sub-c-b04d1286-1706-11e6-84f2-02ee2ddab7fe");
        String[] channels = {worker_1_channel, worker_2_channel, worker_3_channel};

        Supervisor supervisor = initSubscribe(pubnub, channels, supervisor_channel);
        supervisor.start(pubnub);

    }//end main

    public static Supervisor initSubscribe(final Pubnub pn, String[] wchannels, String schannel) {

        Supervisor s = null;

        List<Integer> agg = new ArrayList<Integer>();

        try {
            for (String ch: wchannels) {
                Worker w = new Worker(schannel, pn);
                for (int i = 0; i < w.lower.size(); i++) agg.add(w.lower.get(i));
                pn.subscribe(ch, w);
            }
            s = new Supervisor(wchannels, pn);
            for (int i = 0; i < s.lower.size(); i++) agg.add(s.lower.get(i));
            pn.subscribe(schannel, s);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

        } catch (PubnubException e) {
            e.printStackTrace();
        }
        Collections.sort(agg);
        if (agg.size() % 2 == 0) {
            int len = agg.size();
            double sum = agg.get(len/2 -1) +agg.get(len/2);
            System.out.println("TEST MEDIAN: " + (sum / 2.0));
        }
        else {
            int len = agg.size();
            System.out.println("TEST MEDIAN: " +agg.get(len/2));
        }

        return s;
    }

    private static void notifyUser(Object message) {
        System.out.println(message.toString());
    }

}//end main
