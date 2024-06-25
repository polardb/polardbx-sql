package com.alibaba.polardbx.optimizer.config.table.collation;

import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900ZhData.zh_uca900_weight;

public class Uca900ZhDataTest {

    // Dump from mysql 8.0 strings/ctype-uca.cc
    private static final long[] EXPECTED = {
        2850958330L, 6438609083L, 4142978683L, 3962106001L, 4754908225L, 5292309576L, 3422926846L, 4836417532L,
        7544933508L, 5924603476L, 8693292222L, 8606483789L, 7840250331L, 7411888467L, 9449887972L, 6575618616L,
        5142717414L, 5602762372L, 5887290059L, 5315354049L, 5384595354L, 5424106761L, 6052066055L, 7120159154L,
        6531273761L, 6186607758L, 8644210443L, 4745425151L, 7765354746L, 4998623396L, 5143281183L, 5774307523L,
        3684728077L, 5140343256L, 185921657L, 277021021L, 6703616799L, 223638144L, 256228467L, 329944316L, 362624128L,
        268753847L, 333685134L, 4504080859L, 5093414940L, 7577618673L, 6886479384L, 4833400542L, 4865295452L,
        6038012976L, 12504830666L, 38526237913L, 7440644749L, 6127810278L, 5202307877L, 4449919195L, 5794686586L,
        4286184051L, 5167630513L, 5657673462L, 5098796944L, 6623516320L, 5643421378L, 4534560528L, 3573241380L,
        4649002624L, 4452096132L, 3714648305L, 4185047189L, 4381575286L, 4555778082L, 4120991231L, 4322414597L,
        5076899357L, 3656649886L, 4027894660L, 4386431428L, 3225349248L, 2751434249L, 2747568614L, 2716333235L,
        2653434087L, 2537913840L, 2660156912L, 2708001228L, 2867391751L, 2723984228L, 2576110507L, 2764159060L,
        2725833126L, 2809616128L, 2818154971L, 2712438745L, 2844498050L, 2679085225L, 2712729825L, 2683901777L,
        2687698660L, 2663785864L, 2773155922L, 2601165041L, 2807280304L, 2883868306L, 2676286966L, 2846263063L,
        2751354127L, 2791329365L, 2674320460L, 2805113447L, 2821751980L, 2867968878L, 2771775566L, 3008392034L,
        2931483391L, 2679080722L, 2676457378L, 3053708758L, 2720952646L, 2595459008L, 2830000523L, 2675166368L,
        2976283875L, 2816230291L, 2633156324L, 2800797815L, 2794001508L, 2960900284L, 2706883523L, 2717846026L,
        2743921021L, 2684914632L, 2580661587L, 2836922176L, 2712123269L, 2742628183L, 2833829481L, 2761348638L,
        2606526498L, 2927220325L, 2929913063L, 2683052854L, 2750863076L, 2625907995L, 2726695528L, 2749506890L,
        2901420499L, 2815975089L, 2763389060L, 2675232092L, 2564200789L, 2756276392L, 2657325710L, 2711633713L,
        2792217765L, 2711621616L, 2751001209L, 2694691605L, 2835687899L, 2713479860L, 7895270701L, 5644878976L,
        5670077568L, 5695276160L, 5720474752L, 5042726935L, 5483745792L, 6919910031L, 9302525107L, 5836686037L,
        5232848933L, 7338919240L, 6666111615L, 35519461860L, 2523470832L, 7923333789L, 9756699241L, 18869877754L,
        36844820198L, 4787428177L, 6334314384L, 7602896668L, 4428287469L, 10846778311L, 8396402581L, 6139219533L,
        12749179522L, 5871174144L, 13208861065L, 8345070108L, 7330226213L, 8110776538L, 10119546899L, 8665085360L,
        15049098399L, 6392608856L, 5571365840L, 8781032254L, 13314605008L, 8626326549L, 12222042758L, 9408899506L,
        14920580553L, 13045969824L, 14944145364L, 9402369929L, 5946277760L, 5971476352L, 5996661949L, 11366727443L,
        4635392040L, 15632962662L, 6056324736L, 6081523328L, 6106721920L, 6131920512L, 17563394424L, 6164403072L,
        6189601664L, 17383948076L, 5518098560L, 5543297152L, 14077376767L, 14790280095L, 15045219269L, 19445153287L,
        12254672168L, 1399051888L, 2767124440L, 16920876941L, 14153806312L, 5673713691L, 5578571343L, 4922238549L,
        3873035522L, 630654464L, 655853056L, 9474849827L, 20750621224L, 8545882711L, 17791921610L, 13743625811L,
        3443964967L, 16226860782L, 20370315848L, 489896704L, 515095296L, 540293888L, 3257723690L, 6364173256L,
        11638603576L, 17735218398L, 12639852141L, 12617006905L, 11604346731L, 11956486914L, 11463991533L, 11435708109L,
        10716764681L, 10791252812L, 10859452686L, 11580487095L, 11661578607L, 11508429290L, 10917539039L, 11272789093L,
        11893715129L, 11820226804L, 12305472445L, 11173138829L, 12057958298L, 12979179437L, 13035544720L, 12619728811L,
        12911970143L, 12311418581L, 13223198066L, 13054276927L, 13269606689L, 12388811946L, 12907724063L, 12279235877L,
        11242958387L, 11785033760L, 11815861970L, 10880805481L, 11876582549L, 12771412606L, 12615901967L, 12084267040L,
        13299904632L, 12937546063L, 13923581462L, 13450748098L, 12776144407L, 14283123283L, 14387051097L, 14877487915L,
        13252733986L, 12798252978L, 13521413144L, 13951696332L, 14866559279L, 14839088349L, 15300837849L, 14320776912L,
        13401403710L, 13970081549L, 12406848749L, 12372597137L, 12410463640L, 12824997451L, 14119541048L, 16181126451L,
        14980825147L, 16007777060L, 15377790859L, 15441743427L, 17177525339L, 16610869801L, 17096907136L, 13654257426L,
        12032234983L, 11959828573L, 12772859030L, 16984956156L, 16874893454L, 14149878683L, 14789045540L, 13378080515L,
        13612011711L, 13464923401L, 14352036602L, 12902285030L, 13658848992L, 12557246376L, 14637133580L, 14662307817L,
        16929440447L, 15258363892L, 15037122644L, 14391584751L, 15947640819L, 14331763365L, 12903948775L, 14710162777L,
        15417898480L, 15037783743L, 16519874358L, 16672197058L, 14923916613L, 14483253493L, 14300787886L, 14907515540L,
        15861389508L, 14973001630L, 15735918779L, 14765178541L, 13923222760L, 16782039763L, 15635399966L, 17942960935L,
        16630406159L, 15551654459L, 16302328116L, 16190894300L, 15117869931L, 14492317522L, 15364702939L, 15731028179L,
        16677138336L, 17353662325L, 14569516089L, 16057670340L, 16820030483L, 14480307055L, 13706676657L, 17239227793L,
        15947193357L, 12589017499L, 9732840400L, 10767268037L, 9726889838L, 9589800171L, 10368093462L, 12140883858L,
        10415440192L, 10012650390L, 10004250344L, 11895907413L, 12256526096L, 13405457598L, 9627380845L, 9593410223L,
        10191882391L, 11939727891L, 11304162933L, 10051534275L, 8004740712L, 9141462091L, 8850853490L, 8944576934L,
        9764023359L, 10341123744L, 10431455862L, 10730297597L, 9923081985L, 10252464874L, 10356422208L, 10766719660L,
        12589182949L, 10963503545L, 10629838563L, 11196598854L, 11414369609L, 9303651641L, 10064028142L, 10970909135L,
        13202490911L, 17375006936L, 17345019688L, 17511342569L, 17833716089L, 17987174907L, 17867532799L, 18084475088L,
        18145058050L, 17891138137L, 17932673375L, 18048355447L, 17552239085L, 18678414182L, 19560245760L, 19715679026L,
        19953146560L, 20089646462L, 20958795920L, 4297359532L, 5748595746L, 22943669644L, 10346381612L, 1186709592
    };

    @Test
    public void test() {
        int i = 0;
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[0]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[1]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[2]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[3]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[4]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[5]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[6]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[7]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[8]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[9]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[10]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[11]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[12]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[13]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[14]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[15]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[16]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[17]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[18]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[19]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[20]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[21]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[22]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[23]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[24]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[25]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[26]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[27]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[28]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[29]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[30]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[31]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[32]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[33]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[34]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[35]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[36]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[37]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[38]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[39]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[40]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[41]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[42]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[43]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[44]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[45]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[46]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[47]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[48]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[49]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[50]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[51]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[52]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[53]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[54]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[55]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[56]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[57]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[58]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[59]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[60]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[61]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[62]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[63]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[64]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[65]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[66]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[67]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[68]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[69]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[70]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[71]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[72]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[73]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[74]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[75]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[76]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[77]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[78]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[79]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[80]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[81]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[82]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[83]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[84]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[85]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[86]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[87]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[88]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[89]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[90]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[91]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[92]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[93]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[94]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[95]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[96]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[97]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[98]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[99]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[100]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[101]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[102]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[103]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[104]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[105]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[106]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[107]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[108]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[109]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[110]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[111]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[112]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[113]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[114]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[115]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[116]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[117]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[118]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[119]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[120]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[121]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[122]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[123]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[124]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[125]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[126]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[127]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[128]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[129]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[130]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[131]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[132]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[133]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[134]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[135]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[136]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[137]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[138]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[139]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[140]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[141]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[142]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[143]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[144]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[145]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[146]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[147]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[148]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[149]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[150]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[151]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[152]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[153]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[154]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[155]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[156]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[157]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[158]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[159]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[160]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[161]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[162]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[163]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[164]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[165]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[166]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[167]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[168]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[169]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[170]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[171]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[215]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[249]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[250]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[251]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[252]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[253]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[254]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[255]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[256]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[257]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[258]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[259]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[260]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[261]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[262]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[263]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[264]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[265]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[266]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[267]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[268]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[270]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[272]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[273]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[274]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[275]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[276]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[277]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[278]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[279]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[280]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[282]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[284]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[288]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[289]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[290]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[291]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[292]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[293]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[304]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[305]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[306]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[307]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[308]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[324]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[325]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[326]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[360]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[361]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[362]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[363]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[367]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[432]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[444]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[464]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[465]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[466]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[467]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[468]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[469]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[470]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[471]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[472]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[473]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[474]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[480]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[488]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[489]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[494]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[496]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[497]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[498]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[499]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[500]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[501]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[502]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[503]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[504]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[505]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[512]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[513]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[514]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[515]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[516]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[517]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[518]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[519]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[520]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[521]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[522]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[523]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[524]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[525]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[526]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[527]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[528]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[529]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[530]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[531]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[532]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[533]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[534]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[535]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[536]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[537]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[538]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[539]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[540]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[541]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[542]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[543]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[544]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[545]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[546]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[547]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[548]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[549]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[550]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[551]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[552]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[553]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[554]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[555]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[556]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[557]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[558]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[559]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[560]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[561]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[562]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[563]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[564]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[565]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[566]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[567]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[568]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[569]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[570]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[571]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[572]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[573]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[574]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[575]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[576]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[577]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[578]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[579]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[580]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[581]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[582]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[583]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[584]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[585]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[586]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[587]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[588]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[589]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[590]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[591]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[592]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[593]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[594]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[595]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[596]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[597]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[598]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[599]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[600]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[601]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[602]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[603]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[604]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[605]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[606]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[607]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[608]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[609]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[610]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[611]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[612]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[613]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[614]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[615]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[616]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[617]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[618]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[619]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[620]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[621]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[622]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[623]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[624]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[625]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[626]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[627]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[628]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[629]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[630]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[631]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[632]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[633]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[634]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[635]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[636]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[637]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[638]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[639]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[640]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[641]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[642]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[643]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[644]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[645]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[646]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[647]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[648]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[649]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[650]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[651]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[652]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[653]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[654]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[655]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[656]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[657]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[658]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[659]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[660]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[661]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[662]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[663]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[664]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[665]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[666]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[667]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[668]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[669]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[670]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[671]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[672]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[673]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[674]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[675]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[676]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[677]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[678]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[679]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[680]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[682]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[686]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[687]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[688]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[689]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[690]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[691]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[692]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[693]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[694]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[696]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[708]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[710]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[713]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[715]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[718]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[760]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[761]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[762]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[3584]));
        Assert.assertEquals(EXPECTED[i++], checksum(zh_uca900_weight[3585]));
    }

    public static long checksum(int[] array) {
        long sum = 0;
        for (int i = 0; i < array.length; i++) {
            sum += array[i] * (i + 1);
        }
        return sum;
    }

}
