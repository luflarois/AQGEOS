Program geraCI
  use mpi
  use modMemory
  use modUtils, only: readNamelist,init,encerra
  use modAnalysis, only: analysisNcep,analysisNasa,sm_nasa
  use ModDateUtils, only: date_add_to

  character(len=*), parameter :: cfn1='("'
  character(len=*), parameter :: cfn2='",I4.4,I2.2,' &
              //'I2.2,"_",I2.2,"+",I4.4,I2.2,I2.2,"_",I4.4,".nc4")'
  character(len=*), parameter :: cfn3='",I4.4,I2.2,' &
              //'I2.2,"_",I2.2,"+",I4.4,I2.2,I2.2,"_0030.nc4")'
  integer :: i
  character(len=256) :: innpr,smfile
  integer :: iyy,imm,idd,ihh

  call readNamelist()
 
  call init(tIncrem,lastTime)

 if(id==0) then  
    write(*,fmt='(A,A)')  'prefix    : ',trim(prefix)
    write(*,fmt='(A,I2)') 'imonth1   : ',imonth1
    write(*,fmt='(A,I2)') 'idate1    : ',idate1 
    write(*,fmt='(A,I4)') 'iyear1    : ',iyear1 
    write(*,fmt='(A,I4)') 'itime1    : ',itime1 
    write(*,fmt='(A,I3)') 'ntimes    : ',ntimes 
    write(*,fmt='(A,I2)') 'tincrem   : ',tincrem
    write(*,fmt='(A,A4)') 'source    : ',source
    write(*,fmt='(A,A)')  'outFolder : ',trim(outFolder)
    write(*,fmt='(A,I4)') 'lastTime  : ',lastTime
  endif

  if(source=='NCEP') then

    do i=ini(id),fim(id),tIncrem
       write(innpr,fmt='("'//trim(prefix)//'",I3.3)') i
       call analysisNcep(innpr,id,outFolder)
    enddo

  elseif(source=='NASA') then

    do i=ini(id),fim(id),tIncrem
      call date_add_to(iyear1,imonth1,idate1,itime1,real(i),'h' &
        ,iyy,imm,idd,ihh)
        ihh=ihh/100
      write(innpr,fmt=cfn1//trim(prefix)//cfn2) iYear1,imonth1,idate1 &
            ,itime1,iyy,imm,idd,ihh
      !print *,trim(innpr)
      call analysisNasa(innpr,id,outFolder)

    enddo 
    
    if(id==0) then
		write(smFile,fmt='("'//trim(prefix)//'SM.'//cfn3//'")') iYear1,imonth1, &
            idate1,itime1,iYear1,imonth1,idate1
		call sm_nasa(smFile,outFolder)
	endif
    
  endif 

  i=encerra()

end program geraCI



