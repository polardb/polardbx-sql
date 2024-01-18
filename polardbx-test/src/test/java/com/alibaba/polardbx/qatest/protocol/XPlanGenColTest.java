package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.List;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class XPlanGenColTest extends ReadBaseTestCase {
    private final String TABLE_NAME = "XPlan_get_gencol";

    private static final String TABLE_TEMPLATE = "CREATE TABLE {0} (\n"
        + "        `order_goods_id` varchar(50) NOT NULL,\n"
        + "        `order_id` varchar(50) NOT NULL,\n"
        + "        `order_main_id` varchar(50) NOT NULL,\n"
        + "        `store_id` varchar(50) DEFAULT NULL,\n"
        + "        `store_name` varchar(50) DEFAULT NULL,\n"
        + "        `sell_channel_type` varchar(50) DEFAULT NULL,\n"
        + "        `table_code` varchar(50) DEFAULT NULL,\n"
        + "        `display_category_id` varchar(50) DEFAULT NULL,\n"
        + "        `display_category_name` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_id` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_name` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_code` varchar(50) DEFAULT NULL,\n"
        + "        `member_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_name` varchar(100) NOT NULL,\n"
        + "        `goods_code` varchar(50) DEFAULT NULL,\n"
        + "        `goods_type` varchar(50) DEFAULT NULL,\n"
        + "        `goods_logo_url_small` varchar(255) GENERATED ALWAYS AS (json_unquote(json_extract(`goods_snap_json`, \"$.productLogoUrlSmall\"))) VIRTUAL,\n"
        + "        `product_type` varchar(50) DEFAULT NULL,\n"
        + "        `goods_sell_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_label_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_deposit_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_other_price` decimal(12, 2) DEFAULT NULL,\n"
        + "        `goods_payment_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_qty` int(11) DEFAULT '0',\n"
        + "        `goods_amount` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_package_amount` decimal(18, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_package_payment_amount` decimal(18, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_discount_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_payment_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `is_refund` tinyint(1) DEFAULT '0',\n"
        + "        `goods_refund_qty` int(11) NOT NULL DEFAULT '0',\n"
        + "        `goods_refund_amount` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `package_refund_qty` int(11) NOT NULL DEFAULT '0',\n"
        + "        `package_refund_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `refund_time` datetime DEFAULT NULL,\n"
        + "        `is_enable` tinyint(1) NOT NULL DEFAULT '1',\n"
        + "        `delete_time` bigint(20) NOT NULL DEFAULT '0',\n"
        + "        `update_version` bigint(20) NOT NULL DEFAULT '0',\n"
        + "        `create_time` datetime NOT NULL,\n"
        + "        `update_time` datetime DEFAULT NULL,\n"
        + "        `create_user_id` varchar(50) DEFAULT NULL,\n"
        + "        `update_user_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_snap_json` json NOT NULL,\n"
        + "        `spu_attr_value_json` json DEFAULT NULL,\n"
        + "        `coupon_id_list` varchar(255) DEFAULT NULL,\n"
        + "        `promotion_id_list` varchar(255) DEFAULT NULL,\n"
        + "        `goods_deposit_discount_amount` decimal(18, 2) DEFAULT '0.00',\n"
        + "        `parent_order_goods_id` varchar(50) DEFAULT NULL,\n"
        + "        `is_takeout` tinyint(1) DEFAULT NULL,\n"
        + "        PRIMARY KEY (`order_goods_id`),\n"
        + "        LOCAL KEY `order_id_index` (`order_id`),\n"
        + "        LOCAL KEY `order_main_id_index` (`order_main_id`),\n"
        + "        LOCAL KEY `member_id_index` (`member_id`),\n"
        + "        LOCAL KEY `idx_create_time` (`create_time`)\n"
        + ")";
    private static final String TABLE_TEMPLATE_MYSQL = "CREATE TABLE {0} (\n"
        + "        `order_goods_id` varchar(50) NOT NULL,\n"
        + "        `order_id` varchar(50) NOT NULL,\n"
        + "        `order_main_id` varchar(50) NOT NULL,\n"
        + "        `store_id` varchar(50) DEFAULT NULL,\n"
        + "        `store_name` varchar(50) DEFAULT NULL,\n"
        + "        `sell_channel_type` varchar(50) DEFAULT NULL,\n"
        + "        `table_code` varchar(50) DEFAULT NULL,\n"
        + "        `display_category_id` varchar(50) DEFAULT NULL,\n"
        + "        `display_category_name` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_id` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_name` varchar(50) DEFAULT NULL,\n"
        + "        `manage_category_code` varchar(50) DEFAULT NULL,\n"
        + "        `member_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_name` varchar(100) NOT NULL,\n"
        + "        `goods_code` varchar(50) DEFAULT NULL,\n"
        + "        `goods_type` varchar(50) DEFAULT NULL,\n"
        + "        `goods_logo_url_small` varchar(255) GENERATED ALWAYS AS (json_unquote(json_extract(`goods_snap_json`, \"$.productLogoUrlSmall\"))) VIRTUAL,\n"
        + "        `product_type` varchar(50) DEFAULT NULL,\n"
        + "        `goods_sell_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_label_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_deposit_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_other_price` decimal(12, 2) DEFAULT NULL,\n"
        + "        `goods_payment_price` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_qty` int(11) DEFAULT '0',\n"
        + "        `goods_amount` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `goods_package_amount` decimal(18, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_package_payment_amount` decimal(18, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_discount_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `goods_payment_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `is_refund` tinyint(1) DEFAULT '0',\n"
        + "        `goods_refund_qty` int(11) NOT NULL DEFAULT '0',\n"
        + "        `goods_refund_amount` decimal(12, 2) DEFAULT '0.00',\n"
        + "        `package_refund_qty` int(11) NOT NULL DEFAULT '0',\n"
        + "        `package_refund_amount` decimal(12, 2) NOT NULL DEFAULT '0.00',\n"
        + "        `refund_time` datetime DEFAULT NULL,\n"
        + "        `is_enable` tinyint(1) NOT NULL DEFAULT '1',\n"
        + "        `delete_time` bigint(20) NOT NULL DEFAULT '0',\n"
        + "        `update_version` bigint(20) NOT NULL DEFAULT '0',\n"
        + "        `create_time` datetime NOT NULL,\n"
        + "        `update_time` datetime DEFAULT NULL,\n"
        + "        `create_user_id` varchar(50) DEFAULT NULL,\n"
        + "        `update_user_id` varchar(50) DEFAULT NULL,\n"
        + "        `goods_snap_json` json NOT NULL,\n"
        + "        `spu_attr_value_json` json DEFAULT NULL,\n"
        + "        `coupon_id_list` varchar(255) DEFAULT NULL,\n"
        + "        `promotion_id_list` varchar(255) DEFAULT NULL,\n"
        + "        `goods_deposit_discount_amount` decimal(18, 2) DEFAULT '0.00',\n"
        + "        `parent_order_goods_id` varchar(50) DEFAULT NULL,\n"
        + "        `is_takeout` tinyint(1) DEFAULT NULL,\n"
        + "        PRIMARY KEY (`order_goods_id`),\n"
        + "        KEY `order_id_index` (`order_id`),\n"
        + "        KEY `order_main_id_index` (`order_main_id`),\n"
        + "        KEY `member_id_index` (`member_id`),\n"
        + "        KEY `idx_create_time` (`create_time`)\n"
        + ")";
    private static final String PARTITION_DEF =
        " dbpartition by hash(member_id) tbpartition by hash(member_id) tbpartitions 2";

    private static final String NEW_PARTITION_DEF = " PARTITION BY HASH(SUBSTR(`member_id`,-4)) PARTITIONS 3";

    @Before
    public void initTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));

        String partDef = usingNewPartDb() ? NEW_PARTITION_DEF : PARTITION_DEF;
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(TABLE_TEMPLATE + partDef, quoteSpecialName(TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(TABLE_TEMPLATE_MYSQL, quoteSpecialName(TABLE_NAME)));
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

    private void initData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (`order_goods_id`, `order_id`, `order_main_id`, `store_id`, `store_name`, `sell_channel_type`, `table_code`, `display_category_id`, `display_category_name`, `manage_category_id`, `manage_category_name`, `manage_category_code`, `member_id`, `goods_id`, `goods_name`, `goods_code`, `goods_type`, `product_type`, `goods_sell_price`, `goods_label_price`, `goods_deposit_price`, `goods_other_price`, `goods_payment_price`, `goods_qty`, `goods_amount`, `goods_package_amount`, `goods_package_payment_amount`, `goods_discount_amount`, `goods_payment_amount`, `is_refund`, `goods_refund_qty`, `goods_refund_amount`, `package_refund_qty`, `package_refund_amount`, `refund_time`, `is_enable`, `delete_time`, `update_version`, `create_time`, `update_time`, `create_user_id`, `update_user_id`, `goods_snap_json`, `spu_attr_value_json`, `coupon_id_list`, `promotion_id_list`, `goods_deposit_discount_amount`, `parent_order_goods_id`, `is_takeout`) values ('20230801100712529000000003136', '20230801100712522000000003136', '20230801100712473000000003136', '543', '遇见小面(客村丽影店)', 'TAKESELF', '', '1759341481864200192', '小面/米饭', '1753608359893794816', '干馏面', '01', '1748918919559643136', '1770205009735057408', '小炒牛肉面(测试_打包)', '3900031233', 'NORMAL', 'SINGLE', '0.01', '0.00', '0.01', '0.00', '0.01', '1', '0.01', '0.10', '0.07', '0.00', '0.01', '0', '0', '0.00', '0', '0.00', NULL, '1', '0', '0', '2023-08-01 10:07:13', '2023-08-01 10:07:13', '1748918919559643136', '1748918919559643136', '{\"maxQty\": 0, \"minQty\": 0, \"remark\": \"精选优质牛肉，嫩滑爽口，肉汁丰富，牛肉加面，每一口都超有滋味。\", \"storeId\": \"543\", \"isOnline\": true, \"isSellOut\": false, \"productId\": \"1770205009735057408\", \"sellPrice\": 0.01, \"sortIndex\": 1, \"createTime\": 1690279568000, \"deleteTime\": 0, \"labelPrice\": 0, \"otherPrice\": 0, \"periodJson\": \"[\\\\\"1744643827105267713\\\\\", \\\\\"1744643853146652672\\\\\", \\\\\"1744644487335903232\\\\\", \\\\\"1744655643942322176\\\\\", \\\\\"1744656152697765888\\\\\", \\\\\"1744724809275670528\\\\\"]\", \"updateTime\": 1690279568000, \"productCode\": \"3900031233\", \"productName\": \"小炒牛肉面(测试_打包)\", \"productType\": \"SINGLE\", \"sellWeekDay\": \"1,2,3,4,5,6,7\", \"createUserId\": \"A1744478832686006272\", \"depositPrice\": 0.01, \"isUseBigLogo\": true, \"priceGroupId\": \"1753618342296420352\", \"updateUserId\": \"A1744478832686006272\", \"labelNameList\": \"辣,拌面\", \"updateVersion\": 1, \"createUserName\": \"zwf\", \"finalSellPrice\": 0.01, \"isInSellPeriod\": true, \"storeProductId\": \"1772386588583526400\", \"updateUserName\": \"zwf\", \"finalLabelPrice\": 0, \"sellChannelType\": \"TAKESELF\", \"isSupportDeposit\": true, \"manageCategoryId\": \"1753608359893794816\", \"discountSellValue\": 0, \"displayCategoryId\": \"1759341481864200192\", \"finalDepositPrice\": 0.01, \"productLogoUrlBig\": \"/upload/2022-04-13-17-01-05_62569151e555b_1672910239228.png\", \"discountLabelValue\": 0, \"discountOtherValue\": 0, \"isPromotionProduct\": false, \"manageCategoryCode\": \"01\", \"manageCategoryName\":\"干馏面\", \"menuStoreVersionId\": \"1772386588571992064\", \"displayCategoryName\": \"小面/米饭\", \"productLogoUrlSmall\": \"/upload/2022-04-13-17-01-05_62569151e555b_1672910242080.png\", \"discountDepositValue\": 0, \"productComponentList\": [], \"productDisplayNameCn\": \"小炒牛肉面(测试_打包)\", \"productPackageStrategy\": \"PRODUCT\", \"productPackageFeeAmount\": 0.1}', '[{\"codeStr\": \"2C116X9C0066\", \"storeId\": \"543\", \"deptList\": [{\"deptId\": \"1753608307140984832\", \"deptCode\": \"1753608307140984832\", \"deptName\": \"汤面打包\", \"sortIndex\": 0, \"isPrintSoup\": false, \"isPrintQrcode\": true, \"thirdDeptName\": \"面线\"}], \"isRefund\": false, \"attrPrice\": 0,\"isSellOut\": false, \"productId\": \"1770205009735057408\", \"sellPrice\": 0.01, \"spuRemark\": \"\", \"spuTmplId\": \"1754272697379979264\", \"customList\": [], \"labelPrice\": 0, \"otherPrice\": 0, \"componentId\": \"1770205009737154560\", \"spuTmplCode\": \"10101123\", \"spuTmplName\": \"火腿煎蛋番茄面\", \"depositPrice\": 0.01, \"favoriteList\": [{\"favoriteValueList\": [], \"favoriteTemplateId\": \"1753620733625368576\", \"favoriteTemplateName\": \"免香菜免榨菜\", \"favoriteTemplateDisplayName\": \"喜好\"}, {\"favoriteValueList\": [], \"favoriteTemplateId\": \"1753620601922125824\", \"favoriteTemplateName\": \"免葱免花生\", \"favoriteTemplateDisplayName\": \"喜好\"}], \"orgSellPrice\": 0.01, \"packagePrice\": 0, \"paymentPrice\": 0.01, \"orgLabelPrice\": 0, \"orgOtherPrice\": 0, \"payableAmount\": 0.01, \"paymentAmount\": 0.01, \"spuTmplNameCn\": \"小炒牛肉面(测试_打包)\", \"storeProductId\": \"1772386588583526400\", \"orgDepositPrice\": 0.01, \"attrKeyValueList\": [{\"addPrice\": 0, \"attrKeyId\": \"1753619432242937856\", \"attrKeyName\": \"免辣\", \"attrValueId\": \"1753619432245035008\", \"attrValueName\": \"免辣\", \"payableAmount\": 0, \"paymentAmount\": 0, \"attrKeyDisplayNameCn\": \"口味\", \"depositPaymentAmount\": 0, \"currencyPaymentAmount\": 0, \"attrSellDiscountAmount\": 0, \"attrDepositDiscountAmount\": 0}], \"spuTmplPrintName\": \"火腿煎蛋番茄面(送榨菜)\", \"storeComponentId\": \"1772386588583526401\", \"componentDetailId\": \"1770205009739251712\", \"manageCategoryCode\": \"02\", \"manageCategoryName\": \"汤面\", \"customProductIdList\": [], \"depositPaymentAmount\": 0.01, \"packagePayableAmount\": 0, \"packagePaymentAmount\": 0, \"currencyPaymentAmount\": 0.01, \"spuSellDiscountAmount\": 0, \"storeComponentDetailId\": \"1772386588584574976\", \"spuDepositDiscountAmount\": 0, \"packageDepositPaymentAmount\": 0, \"packageCurrencyPaymentAmount\": 0, \"spuSellPackageDiscountAmount\": 0, \"spuSellProductDiscountAmount\": 0, \"spuDepositPackageDiscountAmount\": 0, \"spuDepositProductDiscountAmount\": 0}]', NULL, '', '0.00', '', NULL)");

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (`order_goods_id`, `order_id`, `order_main_id`, `store_id`, `store_name`, `sell_channel_type`, `table_code`, `display_category_id`, `display_category_name`, `manage_category_id`, `manage_category_name`, `manage_category_code`, `member_id`, `goods_id`, `goods_name`, `goods_code`, `goods_type`, `product_type`, `goods_sell_price`, `goods_label_price`, `goods_deposit_price`, `goods_other_price`, `goods_payment_price`, `goods_qty`, `goods_amount`, `goods_package_amount`, `goods_package_payment_amount`, `goods_discount_amount`, `goods_payment_amount`, `is_refund`, `goods_refund_qty`, `goods_refund_amount`, `package_refund_qty`, `package_refund_amount`, `refund_time`, `is_enable`, `delete_time`, `update_version`, `create_time`, `update_time`, `create_user_id`, `update_user_id`, `goods_snap_json`, `spu_attr_value_json`, `coupon_id_list`, `promotion_id_list`, `goods_deposit_discount_amount`, `parent_order_goods_id`, `is_takeout`) values ('20230801100712529000000003136', '20230801100712522000000003136', '20230801100712473000000003136', '543', '遇见小面(客村丽影店)', 'TAKESELF', '', '1759341481864200192', '小面/米饭', '1753608359893794816', '干馏面', '01', '1748918919559643136', '1770205009735057408', '小炒牛肉面(测试_打包)', '3900031233', 'NORMAL', 'SINGLE', '0.01', '0.00', '0.01', '0.00', '0.01', '1', '0.01', '0.10', '0.07', '0.00', '0.01', '0', '0', '0.00', '0', '0.00', NULL, '1', '0', '0', '2023-08-01 10:07:13', '2023-08-01 10:07:13', '1748918919559643136', '1748918919559643136', '{\"maxQty\": 0, \"minQty\": 0, \"remark\": \"精选优质牛肉，嫩滑爽口，肉汁丰富，牛肉加面，每一口都超有滋味。\", \"storeId\": \"543\", \"isOnline\": true, \"isSellOut\": false, \"productId\": \"1770205009735057408\", \"sellPrice\": 0.01, \"sortIndex\": 1, \"createTime\": 1690279568000, \"deleteTime\": 0, \"labelPrice\": 0, \"otherPrice\": 0, \"periodJson\": \"[\\\\\"1744643827105267713\\\\\", \\\\\"1744643853146652672\\\\\", \\\\\"1744644487335903232\\\\\", \\\\\"1744655643942322176\\\\\", \\\\\"1744656152697765888\\\\\", \\\\\"1744724809275670528\\\\\"]\", \"updateTime\": 1690279568000, \"productCode\": \"3900031233\", \"productName\": \"小炒牛肉面(测试_打包)\", \"productType\": \"SINGLE\", \"sellWeekDay\": \"1,2,3,4,5,6,7\", \"createUserId\": \"A1744478832686006272\", \"depositPrice\": 0.01, \"isUseBigLogo\": true, \"priceGroupId\": \"1753618342296420352\", \"updateUserId\": \"A1744478832686006272\", \"labelNameList\": \"辣,拌面\", \"updateVersion\": 1, \"createUserName\": \"zwf\", \"finalSellPrice\": 0.01, \"isInSellPeriod\": true, \"storeProductId\": \"1772386588583526400\", \"updateUserName\": \"zwf\", \"finalLabelPrice\": 0, \"sellChannelType\": \"TAKESELF\", \"isSupportDeposit\": true, \"manageCategoryId\": \"1753608359893794816\", \"discountSellValue\": 0, \"displayCategoryId\": \"1759341481864200192\", \"finalDepositPrice\": 0.01, \"productLogoUrlBig\": \"/upload/2022-04-13-17-01-05_62569151e555b_1672910239228.png\", \"discountLabelValue\": 0, \"discountOtherValue\": 0, \"isPromotionProduct\": false, \"manageCategoryCode\": \"01\", \"manageCategoryName\":\"干馏面\", \"menuStoreVersionId\": \"1772386588571992064\", \"displayCategoryName\": \"小面/米饭\", \"productLogoUrlSmall\": \"/upload/2022-04-13-17-01-05_62569151e555b_1672910242080.png\", \"discountDepositValue\": 0, \"productComponentList\": [], \"productDisplayNameCn\": \"小炒牛肉面(测试_打包)\", \"productPackageStrategy\": \"PRODUCT\", \"productPackageFeeAmount\": 0.1}', '[{\"codeStr\": \"2C116X9C0066\", \"storeId\": \"543\", \"deptList\": [{\"deptId\": \"1753608307140984832\", \"deptCode\": \"1753608307140984832\", \"deptName\": \"汤面打包\", \"sortIndex\": 0, \"isPrintSoup\": false, \"isPrintQrcode\": true, \"thirdDeptName\": \"面线\"}], \"isRefund\": false, \"attrPrice\": 0,\"isSellOut\": false, \"productId\": \"1770205009735057408\", \"sellPrice\": 0.01, \"spuRemark\": \"\", \"spuTmplId\": \"1754272697379979264\", \"customList\": [], \"labelPrice\": 0, \"otherPrice\": 0, \"componentId\": \"1770205009737154560\", \"spuTmplCode\": \"10101123\", \"spuTmplName\": \"火腿煎蛋番茄面\", \"depositPrice\": 0.01, \"favoriteList\": [{\"favoriteValueList\": [], \"favoriteTemplateId\": \"1753620733625368576\", \"favoriteTemplateName\": \"免香菜免榨菜\", \"favoriteTemplateDisplayName\": \"喜好\"}, {\"favoriteValueList\": [], \"favoriteTemplateId\": \"1753620601922125824\", \"favoriteTemplateName\": \"免葱免花生\", \"favoriteTemplateDisplayName\": \"喜好\"}], \"orgSellPrice\": 0.01, \"packagePrice\": 0, \"paymentPrice\": 0.01, \"orgLabelPrice\": 0, \"orgOtherPrice\": 0, \"payableAmount\": 0.01, \"paymentAmount\": 0.01, \"spuTmplNameCn\": \"小炒牛肉面(测试_打包)\", \"storeProductId\": \"1772386588583526400\", \"orgDepositPrice\": 0.01, \"attrKeyValueList\": [{\"addPrice\": 0, \"attrKeyId\": \"1753619432242937856\", \"attrKeyName\": \"免辣\", \"attrValueId\": \"1753619432245035008\", \"attrValueName\": \"免辣\", \"payableAmount\": 0, \"paymentAmount\": 0, \"attrKeyDisplayNameCn\": \"口味\", \"depositPaymentAmount\": 0, \"currencyPaymentAmount\": 0, \"attrSellDiscountAmount\": 0, \"attrDepositDiscountAmount\": 0}], \"spuTmplPrintName\": \"火腿煎蛋番茄面(送榨菜)\", \"storeComponentId\": \"1772386588583526401\", \"componentDetailId\": \"1770205009739251712\", \"manageCategoryCode\": \"02\", \"manageCategoryName\": \"汤面\", \"customProductIdList\": [], \"depositPaymentAmount\": 0.01, \"packagePayableAmount\": 0, \"packagePaymentAmount\": 0, \"currencyPaymentAmount\": 0.01, \"spuSellDiscountAmount\": 0, \"storeComponentDetailId\": \"1772386588584574976\", \"spuDepositDiscountAmount\": 0, \"packageDepositPaymentAmount\": 0, \"packageCurrencyPaymentAmount\": 0, \"spuSellPackageDiscountAmount\": 0, \"spuSellProductDiscountAmount\": 0, \"spuDepositPackageDiscountAmount\": 0, \"spuDepositProductDiscountAmount\": 0}]', NULL, '', '0.00', '', NULL)");
    }

    @Test
    public void testSubPartIndex() {
        initData();

        final String sql = "SELECT\n"
            + "    goods_snap_json\n"
            + "FROM "
            + quoteSpecialName(TABLE_NAME)
            + " WHERE\n"
            + "    (\n"
            + "        member_id = '1748918919559643136'\n"
            + "        AND order_main_id = '20230801100712473000000003136'\n"
            + "    )";

        // run for 100 times
        for (int i = 0; i < 100; ++i) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        // check trace
        JdbcUtil.executeQuerySuccess(tddlConnection, "trace " + sql);
        final List<List<String>> result =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("show trace", tddlConnection), false,
                ImmutableList.of());
        final String trace = result.get(0).get(11);
        Assert.assertTrue(trace.contains("/*PolarDB-X Connection*/") && trace.contains("plan_digest"));
    }

}
