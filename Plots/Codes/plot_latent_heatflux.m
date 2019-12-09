ccc

dataPath = 'C:\Users\anthi182\Desktop\Micromet_data\Merged_csv';

foret_est = readtable(fullfile(dataPath,'Foret_est_merged_data.csv'));
foret_ouest = readtable(fullfile(dataPath,'Foret_ouest_gapfilled_data.csv'));
berge = readtable(fullfile(dataPath,'Berge_gapfilled_data.csv'));
reservoir = readtable(fullfile(dataPath,'Reservoir_gapfilled_data.csv'));
dateRef = datetime(foret_est.date_index);

% Despiking
foret_est.LE(foret_est.LE > 250 | foret_est.LE<-50) = NaN;
foret_ouest.LE_gap_filled(foret_ouest.LE_gap_filled > 250 | foret_ouest.LE_gap_filled<-50) = NaN;
berge.LE_gap_filled(berge.LE_gap_filled > 250 | berge.LE_gap_filled<-50) = NaN;
reservoir.LE_gap_filled(reservoir.LE_gap_filled > 250 | reservoir.LE_gap_filled<-50) = NaN;

% Conversion coefficient
convCoeff = 86.4/2264;
foret_est_LE = foret_est.LE*convCoeff;
foret_ouest_LE = foret_ouest.LE_gap_filled.*convCoeff;
berge_LE = berge.LE_gap_filled.*convCoeff;
reservoir_LE = reservoir.LE_gap_filled.*convCoeff; 

% LE
figure('Position',[-1920, -175, 1920, 964],'Color','w')
subplot(3,1,1)
hp1 = plot(dateRef, foret_est_LE); hn
hp2 = plot(dateRef, foret_ouest_LE);
LEsmoothLand = smooth(nanmean(cat(2,foret_est_LE,foret_ouest_LE),2),500);
idLEsmoothLand = all(isnan([foret_est_LE, foret_ouest_LE]),2);
LEsmoothLand(idLEsmoothLand) = NaN;
hp3 = plot(dateRef, LEsmoothLand);
set(hp1, 'Color',[51 255 51]./255, 'LineStyle','-.')
set(hp2, 'Color',[51 255 51]./255, 'LineStyle','--')
set(hp3, 'Color',[0  153 0]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('E [mm/jour]')
grid on
title('Foret')

subplot(3,1,2)
hp1 = plot(dateRef, berge_LE); hn
hp2 = plot(dateRef, reservoir_LE);
LEsmoothWater = smooth(nanmean(cat(2,berge_LE,reservoir_LE),2),500);
idLEsmoothWater = all(isnan([berge_LE, reservoir_LE]),2);
LEsmoothWater(idLEsmoothWater) = NaN;
hp3 = plot(dateRef, LEsmoothWater);
set(hp1, 'Color',[102 178 255]./255, 'LineStyle','-.')
set(hp2, 'Color',[102 178 255]./255, 'LineStyle','--')
set(hp3, 'Color',[0   0   205]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('E [mm/jour]')
grid on
title('Reservoir')

subplot(3,1,3)
diffLE = (nanmean(cat(2,berge_LE,reservoir_LE),2) - nanmean(cat(2,foret_est_LE,foret_ouest_LE),2));
diffLEsmooth = smooth(diffLE,500);
diffLEsmooth(isnan(diffLE))=NaN;
hp1 = plot(dateRef, diffLE); hn
hp2 = plot(dateRef, diffLEsmooth);
set(hp1, 'Color',[150 150 150]./255, 'LineStyle','-.')
set(hp2, 'Color',[0   0   0]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('E [mm/jour]')
grid on
title('Différence')

export_fig('../Plots/LE.png')