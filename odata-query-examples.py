from datetime import date, timedelta
from pydash.collections import group_by


### From integration with health services system ###

UPDATE_WINDOW_DAYS = 10

# Get all students where the student or enrollment record has changed within the update window
# This is to give a buffer of time for the script to fail before any back-processing will need to be done
#  to fill in any skipped records
# Also filter out any archive students and any dual enrollment students, as they are not eligible for the clinic
update_window = date.today() - timedelta(days=UPDATE_WINDOW_DAYS)
enrollment_filter = f"e/LastModifiedDateTime gt {update_window} and e/ExpectedStartDate lt {date.today()}"
self.logger.info(f'Getting students modified in the last {UPDATE_WINDOW_DAYS} days...')
students = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/odata/Students'
    f"?$filter=SchoolStatus/Code ne 'ARCAUST' and SchoolStatus/Code ne 'ARCHIVE'"
    f" and (LastModifiedDateTime gt {update_window} or EnrollmentPeriods/any(e: {enrollment_filter}))"
    f"&$select=Id,FirstName,LastName,MiddleName,StudentNumber,StudentEthnicities,MaritalStatus,NiStudent,EmailAddress,Ssn,OriginalExpectedStartDate,MobilePhoneNumber,NickName,Suffix,DateOfBirth,Gender,Veteran,StreetAddress,City,State,PostalCode,PhoneNumber,OtherPhoneNumber"
    f"&$expand=EnrollmentPeriods("
            f"$filter=SchoolStatus/Code ne 'TOPROG';" # Filter out any "Transfer To Other Program" enrollments, as there should always be a subsequent one to use
            f"$select=Id,CreditHoursScheduled,ProgramVersionName,ClockHoursScheduled,Gpa,ProgramVersionName,ExpectedStartDate,ActualStartDate,GraduationDate,SchoolStatusChangeDate,CreatedDateTime;"
            f"$expand=StartTerm($select=Code), GradeLevel($select=Name), ProgramVersion($select=Code),"
            f"  SchoolStatus($expand=SystemSchoolStatus($select=SystemStatusCategory); $select=SystemSchoolStatus),"
            f"  Shift($select=Code)),"
        f"Gender($select=Name),"
        f"Country($select=Name),"
        f"StudentEthnicities($select=Ethnicity; $expand=Ethnicity($select=Name)),"
        f"MaritalStatus($select=Name),"
        f"SchoolStatus($expand=SystemSchoolStatus($select=Id,SystemStatusCategory); $select=Code,Name)", 200)['value']

# Get the current addresses for students from CNS
self.logger.info('Getting current addresses for active students from CNS...')
related_addresses = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/odata/StudentRelationshipAddresses'
    f"?$filter=(AddressEndDate eq null or AddressEndDate gt {date.today()}) and (Student/LastModifiedDateTime gt {update_window} or Student/EnrollmentPeriods/any(e: {enrollment_filter}))"
    f'&$select=StudentId,AddressBeginDate,FirstName,LastName,RelationToStudent,StreetAddress,City,State,PostalCode,PhoneNumber,OtherPhone,EmailAddress,LastModifiedDateTime'
    f'&$expand=AddressType($select=Code,Name)', 500)['value']
related_addresses_by_student_id = group_by(related_addresses, 'StudentId')

# Get all hold groups that students are currently members of
self.logger.info('Getting student hold groups...')
hold_group_members = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/campusnexus/StudentGroupMembers'
    f'?$select=Id, StudentId'
    f'&$filter=IsActive eq true and StudentGroup/HoldCodes/any()'
    f' and (Student/LastModifiedDateTime gt {update_window} or Student/EnrollmentPeriods/any(e: {enrollment_filter}))', 1000)['value']
student_ids_with_holds = set([hold_group_member['StudentId'] for hold_group_member in hold_group_members])


### From integration with bookstore system ###

active_status_filter = get_active_and_admitted_category_filter('SchoolStatus/SystemSchoolStatus/SystemStatusCategory')

# Get all currently active students from CNS
# This is getting the most recent active or attending enrollment (by expected start date) for all active or admitted students
self.logger.info('Getting all active students from CNS...')
students = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/odata/Students'
    f"?$filter={active_status_filter}"
    f"&$select=Id,StudentNumber,FirstName,LastName,EmailAddress,StreetAddress,City,State,PostalCode"
    f"&$expand=Title($select=Name), Country($select=Code), VeteranDetails($select=VeteranAffairsCertificationTypeId),"
        f"EnrollmentPeriods($filter={active_status_filter}; $orderby=ExpectedStartDate desc; $select=Id, SchoolStatus;" 
            f"$expand=ProgramVersion($select=Code),"
            f"SchoolStatus($select=SystemSchoolStatus;"
                f"$expand=SystemSchoolStatus($select=SystemStatusCategory)))", 200)['value']


### From web schedule generation integration ###

# Gets Child-Parent Relationship of all Terms that have not passed and currently have classes assigned to them
term_map = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/campusnexus/TermRelationships?'
    f"$filter=startswith(ParentTerm/Code, 'P') and ParentTerm/EndDate ge {date.today()} and ChildTerm/ClassSections/any()"
    f"&$expand=ParentTerm($select=Code)")['value']

# Get the sections currently scheduled on the child terms of each parent term
# NOTE: You could also go the route of getting all of the currently scheduled sections for all terms, 
#       then split them by parent term using something like the Pydash "group_by", but either would work just about as well
self.logger.info("Getting Section Data...")
term_id_filters = [f"t/TermId eq {id}" for id in child_term_ids]

class_sections_list = self.get_paged(f'{self.__cfg["CNS"]["INTEGRATION_URI"]}ds/odata/ClassSections?'
    f"$filter=IsActive and Terms/any(t: {' or '.join(term_id_filters)})"
    f'&$select=Id,CourseCode,SectionCode,MaximumStudents,NumberRegisteredStudents,FinalCountRegisteredStudents,EnrollmentStatusCreditHours,DeliveryMethodId,StartDate,EndDate,Note'
    f'&$expand=Course($select=Name,CatalogCode),'
    f'Instructor($select=Name),'
    f'Terms($select=TermId,TermName),' 
        f'MeetingDates($expand=Building($select=Name),'
        f'Room($select=RoomNumber); $select=MeetingDate, Status, StartTime, LengthMinutes)', 500)['value']