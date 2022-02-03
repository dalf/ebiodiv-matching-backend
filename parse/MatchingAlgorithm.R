# install.packages(c("dplyr", "stringdist"))
options(width = 160)
options(error = function() {
  traceback(3, max.lines = 10)
})
euclidean <- function(a, b) sqrt(sum((a - b)^2))
library(dplyr)
library(stringdist)

matching <- function(data_plazi, data_museum) {
  #get the required columns, and use the same column names for them
  data_plazi_filtered=select(data_plazi,MatCitId,TaxFamilyEpithet,TaxGenusEpithet,TaxSpeciesEpithet,TaxAuthYear,MatCitLongitude,MatCitLatitude,MatCitYear,MatCitMonth,MatCitSpecimenCount,MatCitCountry,MatCitRegion,MatCitCollector,MatCitElevation,MatCitCollectionCode,MatCitSpecimenCode)
  #rename the column names to be consistent with the museum data
  colnames(data_plazi_filtered)[2]="family"
  colnames(data_plazi_filtered)[3]="genus"
  colnames(data_plazi_filtered)[4]="name"
  colnames(data_plazi_filtered)[5]="auth_year"
  colnames(data_plazi_filtered)[6]="long" 
  colnames(data_plazi_filtered)[7]="lat"
  colnames(data_plazi_filtered)[8]="year_coll"
  colnames(data_plazi_filtered)[9]="month_coll"
  colnames(data_plazi_filtered)[10]="N"
  colnames(data_plazi_filtered)[11]="Country"
  colnames(data_plazi_filtered)[12]="Region"
  colnames(data_plazi_filtered)[13]="Collector"
  colnames(data_plazi_filtered)[14]="Elevation"
  colnames(data_plazi_filtered)[15]="Collection_code"
  colnames(data_plazi_filtered)[16]="Specimen_code"

  #read the museum data
  data_museum_filtered=select(data_museum,OCCUR,TaxFamilyEpithet,TaxGenusEpithet,TaxSpeciesEpithet,TaxAuthYear,MatCitLongitude,MatCitLatitude,MatCitYear,MatCitMonth,MatCitSpecimenCount,MatCitCountry,MatCitRegion,MatCitCollector,MatCitElevation,MatCitCollectionCode,MatCitSpecimenCode)
  #rename the columns to be consistent with the plazi material citations data
  colnames(data_museum_filtered)[2]="family"
  colnames(data_museum_filtered)[3]="genus"
  colnames(data_museum_filtered)[4]="name"
  colnames(data_museum_filtered)[5]="auth_year"
  colnames(data_museum_filtered)[6]="long"
  colnames(data_museum_filtered)[7]="lat"
  colnames(data_museum_filtered)[8]="year_coll"
  colnames(data_museum_filtered)[9]="month_coll"
  colnames(data_museum_filtered)[10]="N"
  colnames(data_museum_filtered)[11]="Country"
  colnames(data_museum_filtered)[12]="Region"
  colnames(data_museum_filtered)[13]="Collector"
  colnames(data_museum_filtered)[14]="Elevation"
  colnames(data_museum_filtered)[15]="Collection_code"
  colnames(data_museum_filtered)[16]="Specimen_code"

  #create a final table that contains matching scores. The table is the core output of the algorithm. I will be exported as JSON file later.
  final_table=matrix(nrow = 0,ncol = 11)
  final_table=as.data.frame(final_table)
  colnames(final_table)=c("OCCUR","MatCitId","String_Score","Taxonomic_Family","Taxonomic_Genus","Taxonomic_Species","Country","State_of_Province","Collected_by","Collection_code","Specimen_code")
  for (i in 1:nrow(data_plazi_filtered)) {
    #find the matching score for the string columns of the material citation "i".   
    string_score_family=1-(stringdist(data_plazi_filtered$family[i],data_museum_filtered$family,method = "jw"))
    string_score_genus=1-(stringdist(data_plazi_filtered$genus[i],data_museum_filtered$genus,method = "jw"))
    string_score_species=1-(stringdist(data_plazi_filtered$name[i],data_museum_filtered$name,method = "jw"))    
    string_score_country=1-(stringdist(data_plazi_filtered$Country[i],data_museum_filtered$Country,method = "jw"))
    string_score_region=1-(stringdist(data_plazi_filtered$Region[i],data_museum_filtered$Region,method = "jw"))
    string_score_collector=1-(stringdist(data_plazi_filtered$Collector[i],data_museum_filtered$Collector,method = "jw"))
    string_score_collection_code=1-(stringdist(data_plazi_filtered$Collection_code[i],data_museum_filtered$Collection_code,method = "jw"))
    string_score_specimen_code=1-(stringdist(data_plazi_filtered$Specimen_code[i],data_museum_filtered$Specimen_code,method = "jw"))

    temporary_frame=matrix(nrow = nrow(data_museum_filtered),ncol = 9)
    temporary_frame=as.data.frame(temporary_frame)
    temporary_frame$V1=string_score_family
    temporary_frame$V2=string_score_genus
    temporary_frame$V3=string_score_species
    temporary_frame$V4=string_score_country
    temporary_frame$V5=string_score_region
    temporary_frame$V6=string_score_collector
    temporary_frame$V7=string_score_collection_code
    temporary_frame$V8=string_score_specimen_code
    temporary_frame$V9=rowMeans(temporary_frame, na.rm=TRUE)
    
    #cummulative string score means the average of all matching scores out of string columns
    cumulative_string_score=as.data.frame(temporary_frame$V9)
    colnames(cumulative_string_score) = c('cumulative_string_score')

    #find the top 5 highest matching scores and the IDS of corresponding museum specimens
    if(max(cumulative_string_score$cumulative_string_score==1))
    { 
      MuseumIDs=which(rowSums(cumulative_string_score == 1) == 1)
    } else if(max(cumulative_string_score$cumulative_string_score< 1))
    {
      MuseumIDs=order(cumulative_string_score$cumulative_string_score, decreasing = T)[1:min(5, nrow(cumulative_string_score))]
    }

    MuseumOCCUR=data_museum_filtered$OCCUR[row.names(data_museum_filtered) %in% MuseumIDs]

    if (length(MuseumOCCUR) == 0) {
      next
    }

    if (length(MuseumOCCUR) != length(MuseumIDs)) {
      print(data_museum_filtered$OCCUR[row.names(data_museum_filtered) %notin% MuseumIDs])
      return(NULL)
    }

    #create a interim table with the values of the selected museum instances, and add them to the final table at the end of the loop
    interim=matrix(nrow = length(MuseumIDs),ncol = 11)
    interim=as.data.frame(interim)
    colnames(interim)=c("OCCUR","MatCitId","String_Score","Taxonomic_Family","Taxonomic_Genus","Taxonomic_Species","Country","State_of_Province","Collected_by","Collection_code","Specimen_code")    
    interim$MatCitId=data_plazi_filtered$MatCitId[i]
    interim$OCCUR=MuseumOCCUR
    interim$String_Score=cumulative_string_score[MuseumIDs,1]
    interim$Taxonomic_Family=string_score_family[MuseumIDs]
    interim$Taxonomic_Genus=string_score_genus[MuseumIDs]
    interim$Taxonomic_Species=string_score_species[MuseumIDs]
    interim$Country=string_score_country[MuseumIDs]
    interim$State_of_Province=string_score_region[MuseumIDs]
    interim$Collected_by=string_score_collector[MuseumIDs]
    interim$Collection_code=string_score_collection_code[MuseumIDs]
    interim$Specimen_code=string_score_specimen_code[MuseumIDs]

    final_table=rbind(final_table,interim)
  }
  #here is a bit data cleaning.. replace the values with -360 and 0 with Not Available (NA)
  data_plazi_filtered$lat[data_plazi_filtered$lat==-360]=NA
  data_plazi_filtered$long[data_plazi_filtered$long==-360]=NA
  data_plazi_filtered$year_coll[data_plazi_filtered$year_coll==0]=NA
  data_plazi_filtered$month_coll[data_plazi_filtered$month_coll==0]=NA
  data_plazi_filtered$Elevation[data_plazi_filtered$Elevation==-6366198]=NA
  data_museum_filtered$Elevation <- gsub("ca.","",data_museum_filtered$Elevation)
  data_museum_filtered$Elevation=as.numeric(as.character(data_museum_filtered$Elevation))

  if (nrow(final_table) == 0) {
    return(final_table)
  }

  for (i in 1:nrow(final_table)) {
    museum_entry=data_museum_filtered[data_museum_filtered$OCCUR==final_table$OCCUR[i],]
    plazi_entry=data_plazi_filtered[data_plazi_filtered$MatCitId==final_table$MatCitId[i],]

    #find euclidian distance for all numeric columns
    final_table$Decimal_Latitude[i]=1-(euclidean(museum_entry$lat,plazi_entry$lat)/abs(max(abs(plazi_entry$lat),abs(museum_entry$lat))))
    final_table$Decimal_Longitude[i]=1-(euclidean(museum_entry$long,plazi_entry$long)/abs(max(abs(plazi_entry$long),abs(museum_entry$long))))
    final_table$Year_collected[i]=1-(euclidean(museum_entry$year_coll,plazi_entry$year_coll)/abs(max(abs(plazi_entry$year_coll),abs(museum_entry$year_coll))))
    final_table$Month_collected[i]=1-(euclidean(museum_entry$month_coll,plazi_entry$month_coll)/abs(max(abs(plazi_entry$month_coll),abs(museum_entry$month_coll))))
    final_table$n_of_specimens[i]=1-(euclidean(museum_entry$N,plazi_entry$N)/abs(max(abs(plazi_entry$N),abs(museum_entry$N))))
    final_table$elevation[i]=1-(euclidean(museum_entry$Elevation,plazi_entry$Elevation)/abs(max(abs(plazi_entry$Elevation),abs(museum_entry$Elevation))))

    c=c(final_table$Decimal_Latitude[i],final_table$Decimal_Longitude[i],final_table$Year_collected[i],final_table$Month_collected[i],final_table$n_of_specimens[i],final_table$elevation[i])
    c=mean(c, na.rm = TRUE)
    #find the cummulative numeric score, which is the mean of score of each numeric columns, similar to the string ones
    final_table$cumulative_numeric_score[i]=c
  }

  #create a new column called final score, that is the average of cummulative score of string columns and cummulative score of the numeric columns
  final_table$final_Score=(final_table$String_Score+final_table$cumulative_numeric_score)/2

  #remove the columns string_score and cummulative_numeric_score, since they are not necessary. The final score has already been calculated and it has its own column
  final_table$String_Score=NULL
  final_table$cumulative_numeric_score=NULL

  #limit the digits of scores
  final_table$Taxonomic_Family=round(final_table$Taxonomic_Family,digits = 3)
  final_table$Taxonomic_Genus=round(final_table$Taxonomic_Genus,digits = 3)
  final_table$Taxonomic_Species=round(final_table$Taxonomic_Species,digits = 3)
  final_table$Country=round(final_table$Country,digits = 3)
  final_table$State_of_Province=round(final_table$State_of_Province,digits = 3)
  final_table$Collected_by=round(final_table$Collected_by,digits = 3)
  final_table$Decimal_Latitude=round(final_table$Decimal_Latitude,digits = 3)
  final_table$Decimal_Longitude=round(final_table$Decimal_Longitude,digits = 3)
  final_table$Year_collected=round(final_table$Year_collected,digits = 3)
  final_table$Specimen_code=round(final_table$Specimen_code,digits = 3)
  final_table$Collection_code=round(final_table$Collection_code,digits = 3)
  final_table$elevation=round(final_table$elevation,digits = 3)
  final_table$Month_collected=round(final_table$Month_collected,digits = 3)
  final_table$n_of_specimens=round(final_table$n_of_specimens,digits = 3)
  final_table$final_Score=round(final_table$final_Score,digits = 3)

  final_table
}
