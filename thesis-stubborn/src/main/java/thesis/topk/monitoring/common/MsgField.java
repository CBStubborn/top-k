package thesis.topk.monitoring.common;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class MsgField {

    //ID
    public static final String OBJ_ID = "obj_id";

    //时间
    public static final String OBJ_TIMESTAMP = "obj_timestamp";

    //本地计数
    public static final String LOCAL_COUNT_MAP = "local_count_map";

    //本地bound值
    public static final String LOCAL_BOUND_VAL = "local_bound_val";

    //全局TopK
    public static final String GLOBAL_TOP_K = "global_top_k";

    //调整因子Map
    public static final String REVISION_MAP = "revision_map";

    //违反集合
    public static final String VIOLATE_SET = "violate_set";

    //resolve集合
    public static final String RESOLVE_SET = "resolve_set";

    public static final String IS_INIT = "is_init";

    public static final String NEW_TOPK = "new_topk";

}