
import pandas as pd
import py4j
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum,max
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window 
from pyspark.sql.functions import col,row_number
import pyspark.sql.functions as f
import json


class AnalyzeCrash:  
    
    def __init__(self,config_file_path):
        self.spark=SparkSession.builder.master("local[1]").appName("app_bcg_assignment").getOrCreate()
        
        with open(config_file_path) as f:
            data=f.read()
        self.config=json.loads(data)
        config=self.config
        # Reading table data 
        self.charges_table=self.spark.read.load(config['charges_table'],format="csv",sep=",",
                         inferSchema="true",header="true")
        self.damages_table=self.spark.read.load(config['damages_table'],format="csv",sep=",",
                             inferSchema="true",header="true")
        self.endorse_table=self.spark.read.load(config['endorse_table'],format="csv",sep=",",
                             inferSchema="true",header="true")
        self.primary_person=self.spark.read.load(config['primary_person'],format="csv",sep=",",
                             inferSchema="true",header="true")
        self.restrict_table=self.spark.read.load(config['restrict_table'],format="csv",sep=",",
                             inferSchema="true",header="true")
        self.unit_table=self.spark.read.load(config['unit_table'],format="csv",sep=",",
                             inferSchema="true",header="true")
        
        # output paths 
        self.save_result_5=config['save_result_5']
        self.save_result_all=config['save_result_all']
    
    
    def analysis1(self):
        '''
        Find the number of crashes(accidents) in which number of persons killed are male
        '''
        primary_person=self.primary_person
        analysis101=primary_person.filter((primary_person.DEATH_CNT>0) & (primary_person.PRSN_GNDR_ID=='MALE'))
        ans1=analysis101.count()
        self.ans1 = ans1
        
        return ans1 

    def analysis2(self):
        '''
        How many two wheelers are booked for crashes??
        '''
        unit_table=self.unit_table
        list_two_wheelers=['MOTORCYCLE','POLICE MOTORCYCLE']
        analysis102=unit_table.filter(unit_table.VEH_BODY_STYL_ID.isin(list_two_wheelers))
        ans2=analysis102.count()
        self.ans2 = ans2
        
        return ans2 

    def analysis3(self):
        '''
        which state has highest number of accidents in which females are involved?
        '''
        primary_person=self.primary_person 
        primary_person=primary_person.dropDuplicates()
        df103=primary_person.filter(primary_person.PRSN_GNDR_ID=='FEMALE').groupBy('DRVR_LIC_STATE_ID').agg(sum('CRASH_ID').alias('sum'))
        ans3=df103.sort('sum',ascending=False).collect()[0].DRVR_LIC_STATE_ID
        self.ans3 = ans3
        
        return ans3 
        
    def analysis4(self):
        '''
        which are the top 5th to 15th vehicle make id's,that contribute to largest number of injuries,
        including deaths.
        '''
        unit_table=self.unit_table 
        unit_table=unit_table.dropDuplicates() 
        df401=unit_table.groupBy('VEH_MAKE_ID').agg(sum('TOT_INJRY_CNT').alias('sum_total_injury'),sum('DEATH_CNT').alias('sum_death_count'))
        df402=df401.withColumn('total_sum',df401.sum_total_injury+df401.sum_death_count)
        df403=df402.sort('total_sum',ascending=False)
        df404=df403.withColumn("index",monotonically_increasing_id())
        ans_5_15=df404.select(df404.VEH_MAKE_ID).filter(df404.index.between(4,14))
        ans4=list(ans_5_15.toPandas()['VEH_MAKE_ID'])
        self.ans4 = ans4
        
        return ans4
        
    def analysis5(self):
        '''
        for all body styles involved in crashes,mention the top ethnic user group of each
        unique body style.
        '''
        primary_person=self.primary_person
        unit_table=self.unit_table
        
        joined_dataset501=unit_table.join(primary_person,(unit_table.CRASH_ID==primary_person.CRASH_ID) &
                                     (unit_table.UNIT_NBR==primary_person.UNIT_NBR),"inner").drop(unit_table.CRASH_ID).drop(unit_table.UNIT_NBR)

        jd_502=joined_dataset501.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').agg(sum('CRASH_ID').alias('body_eth_cnt'))

        windowVehBody=Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("body_eth_cnt").desc())
        ans_df_502=jd_502.withColumn("row",row_number().over(windowVehBody)).filter(col('row')==1)

        #saving file into local
        ans_df_pd=ans_df_502.toPandas()
        ans_df_pd.to_csv(self.save_result_5)
        
        return None 

    def analysis6(self):
        '''
        among the crashed cars,what are the top 5 zip codes with the highest number crashes,
        with alcohol as contributing factor for crash (use driver zip code)
        '''
        unit_table=self.unit_table 
        primary_person=self.primary_person 
        
        joined_dataset602=unit_table.join(primary_person,(unit_table.CRASH_ID==primary_person.CRASH_ID) &
                                     (unit_table.UNIT_NBR==primary_person.UNIT_NBR),"inner").drop(unit_table.CRASH_ID).drop(unit_table.UNIT_NBR)
        joined_dataset_alc=joined_dataset602.filter((joined_dataset602.CONTRIB_FACTR_1_ID=='UNDER INFLUENCE - ALCOHOL') |(joined_dataset602.CONTRIB_FACTR_2_ID=='UNDER INFLUENCE - ALCOHOL'))
        jd_alc=joined_dataset_alc.groupBy('DRVR_ZIP').agg(sum('CRASH_ID').alias('num_crashes')).sort('num_crashes',ascending=False)
        ans6=list(jd_alc.limit(5).toPandas()['DRVR_ZIP'])
        self.ans6 = ans6
        return ans6
        
    def analysis7(self):
        '''
        Count of distinct crash ids where no damaged property was observed and 
        damage level(veh_damg_scl) is above 4, and car avails insurance.
        '''
        damages_table=self.damages_table
        unit_table=self.unit_table 
        damages101=damages_table.groupBy('CRASH_ID').agg(f.concat_ws(",",f.collect_list(damages_table.DAMAGED_PROPERTY)).alias('combined_damages'))
        damages102=damages101.withColumn('length',f.length('combined_damages'))
        damages104=damages102.filter(damages102.length==0)
        damages105=damages104.select('CRASH_ID')
        damages_joined=unit_table.join(damages105,unit_table.CRASH_ID==damages105.CRASH_ID,"inner").drop(unit_table.CRASH_ID)
        result_df=damages_joined.filter(damages_joined.VEH_DMAG_SCL_1_ID.isin(['DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST']))
        ans7=result_df.count() 
        self.ans7 = ans7
        
        return ans7 

    def analysis8(self):
        '''
        determine the top 5 vehicle makes where drivers are chraged with speeding related 
        offences,has licenced drivers,used top 10 used vehicle colours,and has car licenced 
        with the top 25 states with highest number of offences(to be deduced from the data)
        '''
        unit_table=self.unit_table
        primary_person=self.primary_person
        charges_table=self.charges_table 
        
        # joining charges and unit table
        joined_charges_unit=unit_table.join(charges_table,(unit_table.CRASH_ID==charges_table.CRASH_ID) & 
                                            (unit_table.UNIT_NBR==charges_table.UNIT_NBR),"inner").drop(unit_table.CRASH_ID).drop(unit_table.UNIT_NBR)

        # filtering out from the joined table where charge contains SPEED
        charges_filtered=joined_charges_unit.filter(col("CHARGE").contains("SPEED"))

        # filtering out from primary person where Driving licence type is not unlicensed
        licenced_filtered=primary_person.filter(~col('DRVR_LIC_TYPE_ID').contains('UNLICENSED'))

        # joining the licenced filtered with the charges_filtered to find rows where licensed and having speed charges
        three_tables_joined=licenced_filtered.join(charges_filtered,(licenced_filtered.CRASH_ID==charges_filtered.CRASH_ID) & 
                                                   (licenced_filtered.UNIT_NBR==charges_filtered.UNIT_NBR),"inner").drop(licenced_filtered.CRASH_ID).drop(licenced_filtered.UNIT_NBR)

        # finding the top 10 used vehicle colours                               
        top_10_color=unit_table.groupBy('VEH_COLOR_ID').agg(sum('CRASH_ID').alias('sum_crash')).limit(10).select('VEH_COLOR_ID')

        # joining charges table and unit table again,this time to deduce top 25 states
        charge_unit_joined2=charges_table.join(unit_table,(charges_table.CRASH_ID==unit_table.CRASH_ID) & (charges_table.UNIT_NBR==unit_table.UNIT_NBR),'inner').drop(charges_table.UNIT_NBR).drop(charges_table.CRASH_ID)

        #grouping by state to find top 25 states with highest number of offences
        top_25_states=charge_unit_joined2.groupBy('VEH_LIC_STATE_ID').agg(sum('CITATION_NBR').alias('sum_citation')).sort('sum_citation',ascending=False).limit(25).select('VEH_LIC_STATE_ID')

        #filtering out using join the rows,which do not have color in top 10
        color_part_result=three_tables_joined.join(top_10_color,(three_tables_joined.VEH_COLOR_ID==top_10_color.VEH_COLOR_ID),'inner').drop(top_10_color.VEH_COLOR_ID)

        # on the resultant of color part,filtering out the rows which don't fall on top 25 states using join
        top_25_result=color_part_result.join(top_25_states,(color_part_result.VEH_LIC_STATE_ID==top_25_states.VEH_LIC_STATE_ID),'inner').drop(top_25_states.VEH_LIC_STATE_ID)

        #on the resultant spark dataframe,using group by to get top 5 veh make id with highest number of crashes
        final_df=top_25_result.groupBy('VEH_MAKE_ID').agg(sum('CRASH_ID').alias('count_of_crashes')).sort('count_of_crashes',ascending=False).limit(5)

        final_make_id_list=final_df.select('VEH_MAKE_ID').toPandas()['VEH_MAKE_ID']
        ans8=list(final_make_id_list) 
        self.ans8 = ans8
        
        return ans8 
    
    def save_result(self):
        
        dict_ans={}

        dict_ans['analysis1']=[str(self.ans1)]
        dict_ans['analysis2']=[str(self.ans2)]
        dict_ans['analysis3']=[str(self.ans3)]
        dict_ans['analysis4']=[str(self.ans4)]
        dict_ans['analysis5']=["find answer in output5.csv"]
        dict_ans['analysis6']=[str(self.ans6)]
        dict_ans['analysis7']=[str(self.ans7)]
        dict_ans['analysis8']=[str(self.ans8)]

        df_answer=pd.DataFrame.from_dict(dict_ans)
        df_answer.to_csv(self.save_result_all)

        
    
def execute_main():
    input_folder_path='C:\\Users\\kaise\\Desktop\\pyspark-bcg-app-final\\input'
    config_file_path='C:\\Users\\kaise\\Desktop\\pyspark-bcg-app-final\\input\\config_file.txt'
    
    analyze=AnalyzeCrash(config_file_path)
    ans1=analyze.analysis1()
    ans2=analyze.analysis2()
    ans3=analyze.analysis3()
    ans4=analyze.analysis4()
    ans5=analyze.analysis5()
    ans6=analyze.analysis6()
    ans7=analyze.analysis7()
    ans8=analyze.analysis8()
    analyze.save_result()
    
    
    
if __name__ == "__main__":
    execute_main()
    